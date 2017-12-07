#!/usr/bin/env bash

# Build with `mvn -Pcluster clean package`

AIS_TYPE="After201607"
AIS_BASE_DIR="/ais/raw/2016/from201607/"

IHS_PATH="/ihs/routing/2017_ihs_jun.csv"

OUT_BASE_DIR="/processed/routing/ais_filteration_by_ihs/"


FILTER_TYPE="ShipTypeLevel2"

for YY in 07 08 09 10 11 12; do

    for FILTER_VALUE in "Bulk Carriers" "Dry Cargo/Passenger" "Fishing" "Miscellaneous" "Non-Merchant Ships" "Non-Propelled" "Non-Seagoing Merchant Ships" "Non-Ship Structures" "Offshore" "Tankers"; do
        FILTER_VALUE_OUT_NAME=`echo "$FILTER_VALUE" | sed "s: ::g" | sed "s:-::g" | sed "s:/::g"`

        OUT_DIR="${OUT_BASE_DIR}/${FILTER_VALUE_OUT_NAME}/aisfeedukhoposition2016${YY}_${FILTER_VALUE_OUT_NAME}.tsv"

        echo "===================================="
        echo "Current FILTER_VALUE = $FILTER_VALUE"
        echo "Current OUT_DIR = $OUT_DIR"

        spark2-submit \
            --class org.ukho.ds.data_prep.AISFilterationByIHS \
            --executor-memory 16G \
            --driver-memory 4G \
            --master yarn \
            --deploy-mode client \
            --num-executors 5 \
            --executor-cores 3 \
            routing-1.1-SNAPSHOT.jar \
            "$AIS_TYPE" \
            "$AIS_BASE_DIR/aisfeedukhoposition2016${YY}.txt" \
            "$IHS_PATH" \
            "$FILTER_TYPE" \
            "$FILTER_VALUE" \
            "$OUT_DIR" &>  "run_routing-1.1-SNAPSHOT_AISFilterationByIHS_ALL-After201607_${FILTER_VALUE_OUT_NAME}.log"
		
		NUM_LINES=`cat "/mnt/hdfs/$OUT_DIR/part*" | wc -l`
		echo "COMPLETED: NUMBER LINES IN PARTITION FILES = $NUM_LINES"
		echo "===================================="

        hdfs dfs -chmod 777 hdfs:////processed/routing/ais_filteration_by_ihs/*/*.tsv && hdfs dfs -chmod 666 hdfs:////processed/routing/ais_filteration_by_ihs/*/*.tsv/*.csv

    done
done



echo "=== STARTING GROSSTONNAGE COMPUTATION ==="

FILTER_TYPE="GrossTonnage"

for YY in 07 08 09 10 11 12; do
    for FILTER_VALUE in 300 2000; do

        OUT_DIR="${OUT_BASE_DIR}/GrossTonnage_gt${FILTER_VALUE}/aisfeedukhoposition2016${YY}_gt${FILTER_VALUE}.tsv"

        echo "===================================="
        echo "Current FILTER_VALUE = $FILTER_VALUE"
        echo "Current OUT_DIR = $OUT_DIR"

        spark2-submit \
            --class org.ukho.ds.data_prep.AISFilterationByIHS \
            --executor-memory 16G \
            --driver-memory 4G \
            --master yarn \
            --deploy-mode client \
            --num-executors 5 \
            --executor-cores 3 \
            routing-1.1-SNAPSHOT.jar \
            "$AIS_TYPE" \
            "$AIS_BASE_DIR/aisfeedukhoposition2016${YY}.txt" \
            "$IHS_PATH" \
            "$FILTER_TYPE" \
            "$FILTER_VALUE" \
            "$OUT_DIR" &> "run_routing-1.1-SNAPSHOT_AISFilterationByIHS_ALL-After201607_GrossTonnage-gt${FILTER_VALUE}.log"
			
		NUM_LINES=`cat "/mnt/hdfs/$OUT_DIR/part*" | wc -l`
		echo "COMPLETED: NUMBER LINES IN PARTITION FILES = $NUM_LINES"
		echo "===================================="

        hdfs dfs -chmod 777 hdfs:////processed/routing/ais_filteration_by_ihs/*/*.tsv && hdfs dfs -chmod 666 hdfs:////processed/routing/ais_filteration_by_ihs/*/*.tsv/*.csv

    done
done

#ls | while read line; do echo $line; cat $line/*.tsv/part*.csv | wc -l; done
