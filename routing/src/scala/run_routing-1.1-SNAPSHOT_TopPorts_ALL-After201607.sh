#!/usr/bin/env bash

# Build with `mvn -Pcluster clean package`


AVCS_LIMIT_CATALOGUE="/catalogue_limits/avcs/20170913_avcs.csv"

AIS_TYPE="After201607"
AIS_BASE_DIR="/processed/routing/ais_filteration_by_ihs/"

OUT_BASE_DIR="/processed/routing/topports/ais_filteration_by_ihs/"

for YY in 07 08 09 10 11 12; do

    for FILTER_VALUE in "Bulk Carriers" "Dry Cargo/Passenger" "Fishing" "Miscellaneous" "Non-Merchant Ships" "Non-Propelled" "Non-Seagoing Merchant Ships" "Non-Ship Structures" "Offshore" "Tankers"; do
        FILTER_VALUE_OUT_NAME=`echo "$FILTER_VALUE" | sed "s: ::g" | sed "s:-::g" | sed "s:/::g"`

        OUT_DIR="${OUT_BASE_DIR}/${FILTER_VALUE_OUT_NAME}/topports2016${YY}_${FILTER_VALUE_OUT_NAME}.tsv"

        echo "===================================="
        echo "Current FILTER_VALUE_OUT_NAME = $FILTER_VALUE_OUT_NAME"
        echo "Current OUT_DIR = $OUT_DIR"
        echo "===================================="

        spark2-submit \
            --class org.ukho.ds.data_prep.TopPorts \
            --executor-memory 16G \
            --driver-memory 4G \
            --master yarn \
            --deploy-mode client \
            --num-executors 5 \
            --executor-cores 3 \
            routing-1.1-SNAPSHOT.jar \
            "$AVCS_LIMIT_CATALOGUE" \
            "$AIS_BASE_DIR/${FILTER_VALUE_OUT_NAME}/aisfeedukhoposition2016${YY}_${FILTER_VALUE_OUT_NAME}.tsv" \
            "$AIS_TYPE" \
            "$OUT_DIR" &> "run_routing-1.1-SNAPSHOT_TopPorts_ALL-${AIS_TYPE}_${FILTER_VALUE_OUT_NAME}.log"

         hdfs dfs -chmod 777 hdfs:////${OUT_BASE_DIR}/*/*.tsv && hdfs dfs -chmod 666 hdfs:////${OUT_BASE_DIR}/*/*.tsv/*.csv

    done
done


echo "=== STARTING GROSSTONNAGE COMPUTATION ==="


for YY in 07 08 09 10 11 12; do
    for FILTER_VALUE in 300 2000; do

        OUT_DIR="${OUT_BASE_DIR}/GrossTonnage_gt${FILTER_VALUE}/topports2016${YY}_gt${FILTER_VALUE}.tsv"

        echo "===================================="
        echo "Current FILTER_VALUE = $FILTER_VALUE"
        echo "Current OUT_DIR = $OUT_DIR"
        echo "===================================="

        spark2-submit \
            --class org.ukho.ds.data_prep.TopPorts \
            --executor-memory 16G \
            --driver-memory 4G \
            --master yarn \
            --deploy-mode client \
            --num-executors 5 \
            --executor-cores 3 \
            routing-1.1-SNAPSHOT.jar \
            "$AVCS_LIMIT_CATALOGUE" \
            "$AIS_BASE_DIR/GrossTonnage_gt${FILTER_VALUE}/aisfeedukhoposition2016${YY}_gt${FILTER_VALUE}.tsv" \
            "$AIS_TYPE" \
            "$OUT_DIR" &> "run_routing-1.1-SNAPSHOT_TopPorts_ALL-${AIS_TYPE}_GrossTonnage-gt${FILTER_VALUE_OUT_NAME}.log"

        hdfs dfs -chmod 777 hdfs:////${OUT_BASE_DIR}/*/*.tsv && hdfs dfs -chmod 666 hdfs:////${OUT_BASE_DIR}/*/*.tsv/*.csv

    done
done

#ls | while read line; do echo $line; cat $line/*.tsv/part*.csv | wc -l; done
