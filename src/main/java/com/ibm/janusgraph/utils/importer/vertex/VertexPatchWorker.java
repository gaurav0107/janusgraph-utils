/*******************************************************************************
 *   Copyright 2017 IBM Corp. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.ibm.janusgraph.utils.importer.vertex;

import com.ibm.janusgraph.utils.importer.Exception.VertexNotFound;
import com.ibm.janusgraph.utils.importer.util.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class VertexPatchWorker extends Worker {
    private final UUID myID = UUID.randomUUID();

    private final int COMMIT_COUNT;

  //  private final String defaultVertexLabel;
    private String vertexLabelFieldName;
    private String vertexPk;
    private JanusGraphTransaction graphTransaction;
    private long currentRecord;

    private static final Logger log = LoggerFactory.getLogger(VertexPatchWorker.class);

    public VertexPatchWorker(final Iterator<Map<String, String>> records, final Map<String, Object> propertiesMap,
                             final JanusGraph graph) {
        super(records, propertiesMap, graph, "PATCH");

        this.currentRecord = 0;
//        this.defaultVertexLabel = (String) propertiesMap.get(Constants.VERTEX_LABEL_MAPPING);

        this.vertexLabelFieldName = null;

        COMMIT_COUNT = Config.getConfig().getVertexRecordCommitCount();

        if (propertiesMap.values().contains(Constants.VERTEX_LABEL_MAPPING)) {
            // find the vertex
            for (String propName : propertiesMap.keySet()) {
                if (Constants.VERTEX_LABEL_MAPPING.equals(propertiesMap.get(propName))) {
                    this.vertexLabelFieldName = propName;
                    break;
                }
            }
        }
    }

    private void updateRecord(Map<String, String> record) throws Exception {

        JanusGraphVertex v1 = null;
        GraphTraversal<Vertex, Vertex> gt = graphTransaction.traversal().V().has(this.vertexPk, record.get(this.vertexPk));
        if(gt.hasNext()){
            v1 = (JanusGraphVertex) gt.next();
        }else {
            throw new VertexNotFound("vertex with pk: " + this.vertexPk + "=" + record.get(this.vertexPk));
        }

        // set the properties of the vertex
        String value = null;
        for (String column : record.keySet()) {

            String propName = (String) getPropertiesMap().get(column);
            if (propName == null) {
                continue;
            }


            // TODO Convert properties between data types. e.g. Date
            Object convertedValue = BatchHelper.convertPropertyValue(value,
                    graphTransaction.getPropertyKey(propName).dataType());
            try{
                v1.property(propName, convertedValue);
            }catch (Exception e){
                log.error(e.toString());
            }
        }

        if (currentRecord % COMMIT_COUNT == 0) {
            graphTransaction.commit();
            graphTransaction.close();
            graphTransaction = getGraph().newTransaction();
        }
        currentRecord++;
    }

    public UUID getMyID() {
        return myID;
    }

    @Override
    public void run() {
        log.info("Starting new thread " + myID);

        // Start new graph transaction
        graphTransaction = getGraph().newTransaction();
        getRecords().forEachRemaining(new Consumer<Map<String, String>>() {
            @Override
            public void accept(Map<String, String> record) {
                try {
                    updateRecord(record);
                    //graphTransaction.commit();
                } catch (Exception e) {
                    log.error("Thread " + myID + ". Exception during record import.", e);
                    //graphTransaction.rollback();
                }
            }

        });
        graphTransaction.commit();
        graphTransaction.close();

        log.info("Finished thread " + myID);

        notifyListener(WorkerListener.State.Done);
    }

}
