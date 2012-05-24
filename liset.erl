-module(liset).
-export([start/1, start/2, stop/1, get/2, put/3, del/2, set_size/1, % interface for externals
        start/3, loop_lifetime/1, loop_size/1, % internal interface functions
        test_2/0, test_50/0, perftest/0, perftest_lifetime/0, perftest_size/0]). % for tests

% Description

% It's a set of key-value pairs, working in separate thread with limited lifetime
% or amount of items.
%
% Usage:
%    liset:start(test,[limited_lifetime,{lifetime,3600}])   or
%    liset:start(test,[limited_size,{maxsize,5000}])
%    then
%    liset:put(test,key,value), liset:get(test,key)
%    then
%    liset:stop(test)
%
% Lifetime set details:
% When item isn't used for >=lifetime seconds it is removed from set
% Each access restarts lifetime counter from zero
%
% Size set details:
% When size of set (count of items) exceeds the limit the least recently used
% item removed from set downsizing it to suit the limit
%
% virtan@virtan.com

-ifdef(DEBUG).
-define(LOG(X), io:format("~p~n", [X])).
-else.
-define(LOG(X), true).
-endif.

% Server side

default_options() ->
   [limited_size,{maxsize,5000}].

start(Name) ->
   start(Name,default_options()).

start(Name, Options) ->
   Liset = spawn_link(liset, start, [Name, {server_side}, Options]),
   register(Name, Liset),
   {started, Name, Liset}.

stop(Name) ->
   Name ! {stop},
   {stopped, Name}.

start(Name, {server_side}, Options) ->
   {LoopType, Tid, Aux, Consts} = lists:foldl(
       fun (limited_lifetime, {none, none, none, A4}) -> {fun loop_lifetime/1, ets:new(Name,[set,private,{keypos,1}]), none, A4};
           (limited_size, {none, none, none, A4}) -> {fun loop_size/1, ets:new(Name, [set, private, {keypos, 1}]),
                                                      ets:new(list_to_atom(atom_to_list(Name) ++ "_aux__"), [ordered_set, private, {keypos, 1}]), A4};
           ({maxsize, MS}, {A1, A2, A3, none}) -> {A1, A2, A3, MS};
           ({lifetime, LTS}, {A1, A2, A3, none}) -> {A1, A2, A3, LTS};
           (UO, A) -> io:format("liset: unknown option: ~p~n", [UO]), A end,
       {none, none, none, none}, Options),
   if
       (Tid == none) or (Consts == none) or (LoopType == none) ->
           start(Name, {server_side}, default_options());
       true ->
           LoopType({Tid, Aux, Consts, {0}})
   end.

loop_lifetime({Tid, _, LifeTime, _} = AllArgs) ->
   receive
       {Remote, get, Key} ->
           case ets:lookup(Tid,Key) of
               [{Key, _, Object, _}] ->
                   ?LOG({successful_get,Key}),
                   Remote ! {found, Object},
                   ets:update_counter(Tid, Key, {2, 1}),
                   liset:loop_lifetime(AllArgs);
               [] ->
                   ?LOG({unsuccessful_get,Key}),
                   Remote ! {not_found},
                   liset:loop_lifetime(AllArgs)
           end;
       {Remote, put, Key, Object} ->
           ?LOG({put,Key}),
           ets:insert(Tid, {Key, 0, Object, erlang:start_timer(LifeTime*1000, self(), {Key, 0})}),
           if
               Remote /= devnull -> Remote ! {inserted};
               true -> true
           end,
           liset:loop_lifetime(AllArgs);
       {Remote, del, Key} ->
           ?LOG({delete,Key}),
           Remote ! {deleted},
           ets:delete(Tid, Key),
           liset:loop_lifetime(AllArgs);
       {Remote, size} ->
           ?LOG({size}),
           Remote ! {element(2,hd(lists:filter(fun ({size, _}) -> true; (_) -> false end, ets:info(Tid)))), 0},
           liset:loop_lifetime(AllArgs);
       {stop} ->
           ?LOG({stopping,self()}),
           ets:delete(Tid),
           normal;

       % maintenance
       {timeout, Ref, {Key, InitialCount}} ->
           case ets:lookup(Tid,Key) of
               [{Key, InitialCount, _, Ref}] ->
                   ?LOG({timeout_del,Key}),
                   ets:delete(Tid, Key);
               [{Key, NewCount, Object, Ref}] ->
                   ?LOG({timeout_restart,Key}),
                   ets:insert(Tid, {Key, NewCount, Object, erlang:start_timer(LifeTime*1000, self(), {Key, NewCount})});
               [{Key, _, _, _}] ->
                   ?LOG({timeout_ignore,Key});
               _ ->
                   ?LOG({timeout_no_key,Key})
           end,
           liset:loop_lifetime(AllArgs)
   end.

loop_size({Tid, Aux, MaxSize, {GlobalCounter}} = AllArgs) ->
   receive
       {Remote, get, Key} ->
           case ets:lookup(Tid,Key) of
               [{Key, Counter, Object}] ->
                   ?LOG({successful_get,Key}),
                   Remote ! {found, Object},
                   ets:delete(Aux, Counter),
                   NewGlobalCounter = GlobalCounter+1,
                   ets:update_counter(Tid, Key, {2, 0, 0, NewGlobalCounter}),
                   ets:insert(Aux, {NewGlobalCounter, Key});
               [] ->
                   ?LOG({unsuccessful_get,Key}),
                   Remote ! {not_found},
                   NewGlobalCounter = GlobalCounter
           end,
           liset:loop_size({Tid, Aux, MaxSize, {NewGlobalCounter}});
       {Remote, put, Key, Object} ->
           ?LOG({put,Key}),
           NewGlobalCounter = GlobalCounter+1,
           case ets:lookup(Tid, Key) of
               [{Key, OldCounter, _}] ->
                   ets:delete(Tid, Key),
                   ets:delete(Aux, OldCounter);
               [] -> true
           end,
           ets:insert(Tid, {Key, NewGlobalCounter, Object}),
           if
               Remote /= devnull -> Remote ! {inserted};
               true -> true
           end,
           ets:insert(Aux, {NewGlobalCounter, Key}),
           OverMax = element(2,hd(lists:filter(fun ({size, _}) -> true; (_) -> false end, ets:info(Tid)))) > MaxSize,
           if
               OverMax ->
                   ?LOG({decrease_size}),
                   LowCounter = ets:first(Aux),
                   [{_, LowKey}] = ets:lookup(Aux, LowCounter),
                   ets:delete(Tid, LowKey),
                   ets:delete(Aux, LowCounter);
               true -> true
           end,
           liset:loop_size({Tid, Aux, MaxSize, {NewGlobalCounter}});
       {Remote, del, Key} ->
           Remote ! {deleted},
           case ets:lookup(Tid, Key) of
               [{Key, Counter, _}] ->
                   ?LOG({delete, Key}),
                   ets:delete(Tid, Key),
                   ets:delete(Aux, Counter);
               [] ->
                   ?LOG({delete_none, Key})
           end,
           liset:loop_size(AllArgs);
       {Remote, size} ->
           ?LOG({size}),
           Remote ! { element(2,hd(lists:filter(fun ({size, _}) -> true; (_) -> false end, ets:info(Tid)))),
                      element(2,hd(lists:filter(fun ({size, _}) -> true; (_) -> false end, ets:info(Aux))))},
           liset:loop_size(AllArgs);
       {stop} ->
           ?LOG({stopping,self()}),
           ets:delete(Tid),
           ets:delete(Aux),
           normal
   end.



% Client side

get(Name, Key) ->
   Name ! {self(), get, Key},
   receive
       Answer -> Answer
   end.

put(Name, Key, Object) ->
   Name ! {self(), put, Key, Object},
   receive
       Answer -> Answer
   end.

del(Name, Key) ->
   Name ! {self(), del, Key},
   receive
       Answer -> Answer
   end.

set_size(Name) ->
   Name ! {self(), size},
   receive
       Answer -> Answer
   end.


% Tests

perftest() ->
   perftest_lifetime(),
   perftest_size().

perftest_lifetime() ->
   io:format("limited lifetime set, lifetime = 50 seconds~n"),
   liset:start(test, [limited_lifetime,{lifetime,50}]),
   io:format("testing by test_2 (1x put + 2x optional get + 1x stable get)~n"),
   perftest:comprehensive(500000, fun test_2/0),
   io:format("set_size = ~p~n",[liset:set_size(test)]),
   io:format("waiting 102 seconds to check cleaning...~n"),
   receive after 102000 ->
       THS = liset:set_size(test),
       io:format("set_size = ~p~n",[THS]),
       {0, 0} = THS
   end,
   io:format("testing by test_50 (1x put + 50x optional get + 1x stable get)~n"),
   perftest:comprehensive(100000, fun test_50/0),
   io:format("set_size = ~p~n",[liset:set_size(test)]),
   io:format("waiting 102 seconds to check cleaning...~n"),
   receive after 102000 ->
       THS2 = liset:set_size(test),
       io:format("set_size = ~p~n",[THS2]),
       {0, 0} = THS2
   end,
   liset:stop(test),
   io:format("test finished~n").

perftest_size() ->
   io:format("limited size set, maxsize = 5000 objects~n"),
   liset:start(test, [limited_size,{maxsize,5000}]),
   io:format("testing by test_2 (1x put + 2x optional get + 1x stable get)~n"),
   perftest:comprehensive(500000, fun test_2/0),
   io:format("set_size = ~p~n",[liset:set_size(test)]),
   io:format("testing by test_50 (1x put + 50x optional get + 1x stable get)~n"),
   perftest:comprehensive(100000, fun test_50/0),
   io:format("set_size = ~p~n",[liset:set_size(test)]),
   liset:stop(test),
   io:format("test finished~n").

test_50() ->
   run_tests(test, 50),
   true.

test_2() ->
   run_tests(test, 2),
   true.

multiply_gets(_, 0) ->
   done;
multiply_gets(Hash, Multiplicator) ->
   get(Hash,random:uniform(1000000)),
   multiply_gets(Hash, Multiplicator-1).

run_tests(Hash, Multiplicator) ->
   Key = random:uniform(1000000),
   {inserted} = put(Hash,Key,Key), % put random key
   multiply_gets(Hash, Multiplicator), % get random key many times
   {found, Key} = get(Hash,Key). % get existent key
