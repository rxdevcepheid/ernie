-module(ernie_server).
-behaviour(gen_server).
-include_lib("ernie.hrl").

%% api
-export([start_link/1, start/1, process/1, enqueue_request/1, kick/0, fin/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

process(Sock) ->
  gen_server:cast(?MODULE, {process, Sock}).

enqueue_request(Request) ->
  gen_server:call(?MODULE, {enqueue_request, Request}).

kick() ->
  gen_server:cast(?MODULE, kick).

fin() ->
  gen_server:cast(?MODULE, fin).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Port, Configs]) ->
  process_flag(trap_exit, true),
  lager:info("~p starting~n", [?MODULE]),
  Mode = case application:get_env(ernie_server, ssl) of
    {ok, true} -> ssl;
    _          -> gen_tcp
  end,
  {ok, LSock} = try_listen(Port, 500, Mode),
  spawn(fun() -> loop(LSock, Mode) end),
  Map = init_map(Configs),
  io:format("pidmap = ~p~n", [Map]),
  {ok, #state{lsock = LSock, map = Map, mode = Mode}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({enqueue_request, Request}, _From, State) ->
  case Request#request.priority of
    high ->
      Hq2 = queue:in(Request, State#state.hq),
      Lq2 = State#state.lq;
    low ->
      Hq2 = State#state.hq,
      Lq2 = queue:in(Request, State#state.lq)
  end,
  {reply, ok, State#state{hq = Hq2, lq = Lq2}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({process, Sock}, State) ->
  Log = #log{hq = queue:len(State#state.hq),
             lq = queue:len(State#state.lq),
             taccept = erlang:now()},
  Request = #request{sock = Sock, log = Log},
  spawn(fun() -> receive_term(Request, State) end),
  lager:debug("Spawned receiver~n", []),
  {noreply, State};
handle_cast(kick, State) ->
  case queue:out(State#state.hq) of
    {{value, Request}, Hq2} ->
      State2 = process_request(Request, hq, Hq2, State),
      {noreply, State2};
    {empty, _Hq} ->
      case queue:out(State#state.lq) of
        {{value, Request}, Lq2} ->
          State2 = process_request(Request, lq, Lq2, State),
          {noreply, State2};
        {empty, _Lq} ->
          {noreply, State}
      end
  end;
handle_cast(fin, State) ->
  Listen = State#state.listen,
  Count = State#state.count,
  ZCount = State#state.zcount + 1,
  lager:debug("Fin; Listen = ~p (~p/~p)~n", [Listen, Count, ZCount]),
  case Listen =:= false andalso ZCount =:= Count of
    true -> halt();
    false -> {noreply, State#state{zcount = ZCount}}
  end;
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(Msg, State) ->
  lager:error("Unexpected message: ~p~n", [Msg]),
  {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

%%====================================================================
%% Internal
%%====================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Module mapping

init_map(Configs) ->
  lists:map((fun extract_mapping/1), Configs).

extract_mapping(Config) ->
  Id = proplists:get_value(id, Config),
  Mod = proplists:get_value(module, Config),
  {Mod, Id}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Listen and loop

try_listen(Port, 0, _Mode) ->
  lager:error("Could not listen on port ~p~n", [Port]),
  {error, "Could not listen on port"};
try_listen(Port, Times, Mode) ->
  Res = case Mode of
    ssl ->
      {ok, Cacertfile} = application:get_env(ernie_server, cacertfile),
      {ok, Certfile}   = application:get_env(ernie_server, certfile),
      {ok, Keyfile}    = application:get_env(ernie_server, keyfile),
      ssl:listen(Port, [binary, {packet, 4}, {active, false}, {reuseaddr, true}, {backlog, 128},
        {cacertfile, Cacertfile},
        {certfile, Certfile},
        {keyfile, Keyfile}
      ]);
    gen_tcp ->
      gen_tcp:listen(Port, [binary, {packet, 4}, {active, false}, {reuseaddr, true}, {backlog, 128}])
  end,
  case Res of
    {ok, LSock} ->
      lager:info("Listening on port ~p~n", [Port]),
      % gen_tcp:controlling_process(LSock, ernie_server),
      {ok, LSock};
    {error, Reason} ->
      lager:info("Could not listen on port ~p: ~p~n", [Port, Reason]),
      timer:sleep(5000),
      try_listen(Port, Times - 1, Mode)
  end.

loop(LSock, Mode) ->
  Accept = case Mode of
    ssl     -> ssl:transport_accept(LSock);
    gen_tcp -> gen_tcp:accept(LSock)
  end,
  case Accept of
    {error, closed} ->
      lager:debug("Listen socket closed~n", []),
      timer:sleep(infinity);
    {error, Error} ->
      lager:debug("Connection accept error: ~p~n", [Error]),
      loop(LSock, Mode);
    {ok, Sock} ->
      lager:debug("Accepted socket: ~p~n", [Sock]),
      case Mode of
        ssl     -> ok = ssl:ssl_accept(Sock);
        gen_tcp -> ok
      end,
      ernie_server:process(Sock),
      loop(LSock, Mode)
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Receive and process

receive_term(Request, State) ->
  Sock = Request#request.sock,
  Mode = State#state.mode,
  case Mode:recv(Sock, 0) of
    {ok, BinaryTerm} ->
      lager:debug("Got binary term: ~p~n", [BinaryTerm]),
      Term = binary_to_term(BinaryTerm),
      lager:info("Got term: ~p~n", [Term]),
      case Term of
        {call, '__admin__', Fun, Args} ->
          ernie_admin:process(Sock, Fun, Args, State);
        {info, Command, Args} ->
          Infos = Request#request.infos,
          Infos2 = [BinaryTerm | Infos],
          Request2 = Request#request{infos = Infos2},
          Request3 = process_info(Request2, Command, Args),
          receive_term(Request3, State);
        _Any ->
          Request2 = Request#request{action = BinaryTerm},
          close_if_cast(Term, Request2, State),
          ernie_server:enqueue_request(Request2),
          ernie_server:kick()
      end;
    {error, closed} ->
      ok = Mode:close(Sock)
  end.

process_info(Request, priority, [Priority]) ->
  Request#request{priority = Priority};
process_info(Request, _Command, _Args) ->
  Request.

process_request(Request, Priority, Q2, State) ->
  ActionTerm = bert:decode(Request#request.action),
  {_Type, Mod, _Fun, _Args} = ActionTerm,
  Specs = lists:filter(fun({X, _Id}) -> Mod =:= X end, State#state.map),
  case Specs of
    [] -> no_module(Mod, Request, Priority, Q2, State);
    _Else -> process_module(ActionTerm, Specs, Request, Priority, Q2, State)
  end.

no_module(Mod, Request, Priority, Q2, State) ->
  lager:debug("No such module ~p~n", [Mod]),
  Sock = Request#request.sock,
  Class = <<"ServerError">>,
  Message = list_to_binary(io_lib:format("No such module '~p'", [Mod])),
  Mode = State#state.mode,
  Mode:send(Sock, term_to_binary({error, [server, 0, Class, Message, []]})),
  ok = Mode:close(Sock),
  finish(Priority, Q2, State).

process_module(ActionTerm, [], Request, Priority, Q2, State) ->
  {_Type, Mod, Fun, _Args} = ActionTerm,
  lager:debug("No such function ~p:~p~n", [Mod, Fun]),
  Sock = Request#request.sock,
  Class = <<"ServerError">>,
  Message = list_to_binary(io_lib:format("No such function '~p:~p'", [Mod, Fun])),
  Mode = State#state.mode,
  Mode:send(Sock, term_to_binary({error, [server, 0, Class, Message, []]})),
  ok = Mode:close(Sock),
  finish(Priority, Q2, State);
process_module(ActionTerm, Specs, Request, Priority, Q2, State) ->
  [{_Mod, Id} | OtherSpecs] = Specs,
  case Id of
    native ->
      lager:debug("Dispatching to native module~n", []),
      {_Type, Mod, Fun, Args} = ActionTerm,
      case erlang:function_exported(Mod, Fun, length(Args)) of
        false ->
          lager:debug("Not found in native module ~p~n", [Mod]),
          process_module(ActionTerm, OtherSpecs, Request, Priority, Q2, State);
        true ->
          PredFun = list_to_atom(atom_to_list(Fun) ++ "_pred"),
          lager:debug("Checking ~p:~p(~p) for selection.~n", [Mod, PredFun, Args]),
          case erlang:function_exported(Mod, PredFun, length(Args)) of
            false ->
              lager:debug("No such predicate function ~p:~p(~p).~n", [Mod, PredFun, Args]),
              process_native_request(ActionTerm, Request, Priority, Q2, State);
            true ->
              case apply(Mod, PredFun, Args) of
                false ->
                  lager:debug("Predicate ~p:~p(~p) returned false.~n", [Mod, PredFun, Args]),
                  process_module(ActionTerm, OtherSpecs, Request, Priority, Q2, State);
                true ->
                  lager:debug("Predicate ~p:~p(~p) returned true.~n", [Mod, PredFun, Args]),
                  process_native_request(ActionTerm, Request, Priority, Q2, State)
              end
          end
      end;
    ValidPid when is_pid(ValidPid) ->
      lager:debug("Found external pid ~p~n", [ValidPid]),
      process_external_request(ValidPid, Request, Priority, Q2, State)
  end.

close_if_cast(ActionTerm, Request, State) ->
  case ActionTerm of
    {cast, _Mod, _Fun, _Args} ->
      Sock = Request#request.sock,
      Mode = State#state.mode,
      Mode:send(Sock, term_to_binary({noreply})),
      ok = Mode:close(Sock),
      lager:debug("Closed cast.~n", []);
    _Any ->
      ok
  end.

finish(Priority, Q2, State) ->
  case Priority of
    hq -> State#state{hq = Q2};
    lq -> State#state{lq = Q2}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Native

process_native_request(ActionTerm, Request, Priority, Q2, State) ->
  Count = State#state.count,
  State2 = State#state{count = Count + 1},
  lager:debug("Count = ~p~n", [Count + 1]),
  Log = Request#request.log,
  Log2 = Log#log{type = native, tprocess = erlang:now()},
  Request2 = Request#request{log = Log2},
  spawn(fun() -> ernie_native:process(ActionTerm, Request2, State) end),
  finish(Priority, Q2, State2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% External

process_external_request(Pid, Request, Priority, Q2, State) ->
  Count = State#state.count,
  State2 = State#state{count = Count + 1},
  lager:debug("Count = ~p~n", [Count + 1]),
  case asset_pool:lease(Pid) of
    {ok, Asset} ->
      lager:debug("Leased asset for pool ~p~n", [Pid]),
      Log = Request#request.log,
      Log2 = Log#log{type = external, tprocess = erlang:now()},
      Request2 = Request#request{log = Log2},
      spawn(fun() -> process_now(Pid, Request2, Asset, State) end),
      finish(Priority, Q2, State2);
    empty ->
      State
  end.

process_now(Pid, Request, Asset, State) ->
  try unsafe_process_now(Request, Asset, State) of
    _AnyResponse ->
      Log = Request#request.log,
      Log2 = Log#log{tdone = erlang:now()},
      Request2 = Request#request{log = Log2},
      ernie_access_logger:acc(Request2)
  catch
    AnyClass:AnyError ->
      Log = Request#request.log,
      Log2 = Log#log{tdone = erlang:now()},
      Request2 = Request#request{log = Log2},
      ernie_access_logger:err(Request2, "External process error ~w: ~w", [AnyClass, AnyError])
  after
    asset_pool:return(Pid, Asset),
    ernie_server:fin(),
    ernie_server:kick(),
    lager:debug("Returned asset ~p~n", [Asset]),
    Mode = State#state.mode,
    Mode:close(Request#request.sock),
    lager:debug("Closed socket ~p~n", [Request#request.sock])
  end.

unsafe_process_now(Request, Asset, State) ->
  BinaryTerm = Request#request.action,
  Term = binary_to_term(BinaryTerm),
  case Term of
    {call, Mod, Fun, Args} ->
      lager:debug("Calling ~p:~p(~p)~n", [Mod, Fun, Args]),
      Sock = Request#request.sock,
      {asset, Port, Token} = Asset,
      lager:debug("Asset: ~p ~p~n", [Port, Token]),
      {ok, Data} = port_wrapper:rpc(Port, BinaryTerm),
      Mode = State#state.mode,
      ok = Mode:send(Sock, Data);
    {cast, Mod, Fun, Args} ->
      lager:debug("Casting ~p:~p(~p)~n", [Mod, Fun, Args]),
      {asset, Port, Token} = Asset,
      lager:debug("Asset: ~p ~p~n", [Port, Token]),
      {ok, _Data} = port_wrapper:rpc(Port, BinaryTerm)
  end.
