-module(vordb_dirty_tracker_test_helper).
-export([whereis_dt/0]).

whereis_dt() ->
    case whereis(vordb_dirty_tracker) of
        undefined -> false;
        _ -> true
    end.
