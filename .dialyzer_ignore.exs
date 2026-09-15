[
  # mongodb_driver typespecs for insert_many/create_indexes success paths are incomplete
  # under Dialyzer, so {:ok, _} clauses look unreachable even though they run at runtime.
  {"lib/rheo/backend/mongo.ex", :pattern_match},
  {"lib/rheo/backend/mongo.ex", :pattern_match_cov}
]
