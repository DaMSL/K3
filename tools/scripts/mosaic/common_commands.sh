# Common commands used

# Light queries
queries='1 3 4 6 10 11a 12 14 17 18 18a 19 22a'

# Compile queries
for i in $queries; do ./tools/scripts/mosaic/run.rb -1 -3 -w ./mosaic/tpch$i ../K3-Mosaic/tests/queries/tpch/query$i.sql; done

# Run queries with small dataset, in single-machine multicore mode (qp3)
dataset='0.01'
for i in $queries; do ./tools/scripts/mosaic/run.rb -5 -w ./mosaic/tpch$i --multicore -k /local/data/mosaic/sf-$dataset/vanilla/agenda.tbl ../K3-Mosaic/tests/queries/tpch/query$i.sql; done
