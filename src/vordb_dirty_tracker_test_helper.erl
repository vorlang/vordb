-module(vordb_dirty_tracker_test_helper).
-export([whereis_dt/0]).

whereis_dt() ->
    %% ETS-based tracker — check if tables exist
    case ets:whereis(vordb_dirty) of
        undefined -> false;
        _ -> true
    end.
