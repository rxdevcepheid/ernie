-module(ernie_native).
-export([process/3]).
-include_lib("ernie.hrl").

process(ActionTerm, Request, State) ->
  {_Type, Mod, Fun, Args} = ActionTerm,
  Sock = Request#request.sock,
  Mode = State#state.mode,
  lager:debug("Calling ~p:~p(~p)~n", [Mod, Fun, Args]),
  try apply(Mod, Fun, Args) of
    Result ->
      lager:debug("Result was ~p~n", [Result]),
      Data = bert:encode({reply, Result}),
      Mode:send(Sock, Data)
  catch
    AnyClass:Error ->
      BError = list_to_binary(io_lib:format("~p:~p", [AnyClass, Error])),
      Trace = erlang:get_stacktrace(),
      BTrace = lists:map(fun(X) -> list_to_binary(io_lib:format("~p", [X])) end, Trace),
      Data = term_to_binary({error, [user, 0, <<"RuntimeError">>, BError, BTrace]}),
      Mode:send(Sock, Data)
  end,
  ok = Mode:close(Sock),
  ernie_server:fin(),
  Log = Request#request.log,
  Log2 = Log#log{tdone = erlang:now()},
  Request2 = Request#request{log = Log2},
  ernie_access_logger:acc(Request2).
