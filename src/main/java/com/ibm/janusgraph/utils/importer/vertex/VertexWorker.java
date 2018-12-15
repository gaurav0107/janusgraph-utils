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

import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.ibm.janusgraph.utils.importer.Exception.VertexNotFound;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;

import com.ibm.janusgraph.utils.importer.util.BatchHelper;
import com.ibm.janusgraph.utils.importer.util.Config;
import com.ibm.janusgraph.utils.importer.util.Constants;
import com.ibm.janusgraph.utils.importer.util.Worker;
import com.ibm.janusgraph.utils.importer.util.WorkerListener;

public class VertexWorker extends Worker {
    private final UUID myID = UUID.randomUUID();

    private final int COMMIT_COUNT;

    private final String defaultVertexLabel;
    private String vertexLabelFieldName;
    private JanusGraphTransaction graphTransaction;
    private long currentRecord;
    private String jobType;
    private String vertexPk;

    private static final Logger log = LoggerFactory.getLogger(VertexWorker.class);

    public VertexWorker(final Iterator<Map<String, String>> records, final Map<String, Object> propertiesMap,
                        final JanusGraph graph, String jobType) {
        super(records, propertiesMap, graph, jobType);

        this.currentRecord = 0;
        this.defaultVertexLabel = (String) propertiesMap.get(Constants.VERTEX_LABEL_MAPPING);
        this.vertexLabelFieldName = null;
        this.jobType = jobType;
        this.vertexPk = (String)propertiesMap.get(Constants.VERTEX_PK);

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

    private String getVertexLabel(Map<String, String> record){
        String vertexLabel = defaultVertexLabel;
        if (vertexLabelFieldName != null) {
            vertexLabel = record.get(vertexLabelFieldName);
        }
        return vertexLabel;
    }

    private JanusGraphVertex addVertexProperties(JanusGraphVertex v, Map<String, String> record) throws ParseException {

        // set the properties of the vertex
        String value = null;

        for (String column : record.keySet()) {
            try {
                value = record.get(column);
            }catch (Exception e){
                System.out.println("here is the problem : " + column);
                System.out.println("here is the problem : " + record.get(column));
                throw e;
            }
            // If value="" or it is a vertex label then skip it
            if (value == null || value.length() == 0 || column.equals(vertexLabelFieldName))
                continue;

            String propName = (String) getPropertiesMap().get(column);
            if (propName == null) {
                continue;
            }

            if (!v.properties(propName).hasNext()) {
                // TODO Convert properties between data types. e.g. Date
                Object convertedValue = BatchHelper.convertPropertyValue(value,
                        graphTransaction.getPropertyKey(propName).dataType());
                v.property(propName, convertedValue);
            }
        }
        return v;
    }


    private JanusGraphVertex updateVertexProperties(JanusGraphVertex v, Map<String, String> record) throws ParseException {

        // set the properties of the vertex
        String value = null;

        for (String column : record.keySet()) {
           try {
                value = record.get(column);
            }catch (Exception e){
                System.out.println("here is the problem : " + column);
                System.out.println("here is the problem : " + record.get(column));
                throw e;
            }
            // If value="" or it is a vertex label then skip it
            if (value == null || value.length() == 0 || column.equals(vertexLabelFieldName))
                continue;

            String propName = (String) getPropertiesMap().get(column);
            if (propName == null) {
                continue;
            }
            // TODO Convert properties between data types. e.g. Date
            Object convertedValue = BatchHelper.convertPropertyValue(value, graphTransaction.getPropertyKey(propName).dataType());
            try{
                v.property(propName, convertedValue);
            }catch (Exception e){
                log.error(e.toString());
            }
            System.out.println(v.values().toString());

        }
        return v;
    }

    private JanusGraphVertex addNewVertex(Map<String, String> record) throws Exception {
        String vertexLabel = getVertexLabel(record);
        System.out.println("adding new vertex");
        JanusGraphVertex v = graphTransaction.addVertex(vertexLabel);
        return v;
    }

    private void postRecord(Map<String, String> record){
        log.info("calling post record");
        try{
            JanusGraphVertex v = addNewVertex(record);
            updateVertexProperties(v, record);
            if (currentRecord % COMMIT_COUNT == 0) {
                graphTransaction.commit();
            }
            currentRecord++;
        }catch (Exception e) {
            log.error(e.toString());
            graphTransaction.rollback();
        }
        graphTransaction.close();
        graphTransaction = getGraph().newTransaction();

    }


    private void putRecord(Map<String, String> record) throws Exception {
        log.info("calling put record");
        JanusGraphVertex v = null;

        String vertexLabel = defaultVertexLabel;
        if (vertexLabelFieldName != null) {
            vertexLabel = record.get(vertexLabelFieldName);
        }
        try{
            GraphTraversal<Vertex, Vertex> gt = graphTransaction.traversal().V().has(vertexPk, record.get(vertexPk));
            if(gt.hasNext()){
                v = (JanusGraphVertex) gt.next();
            }else {
                v = addNewVertex(record);
            }
            v = updateVertexProperties(v, record);
            graphTransaction.commit();
        }catch (Exception e){
            log.error(e.toString());
            graphTransaction.rollback();
            throw e;
        }
        graphTransaction.close();
        graphTransaction = getGraph().newTransaction();

    }

    private void patchRecord(Map<String, String> record) throws Exception {
        try {
            JanusGraphVertex v = null;
            GraphTraversal<Vertex, Vertex> gt = graphTransaction.traversal().V().has(vertexPk, record.get(vertexPk));
            if (gt.hasNext()) {
                v = (JanusGraphVertex) gt.next();
                log.info("Vertex found updating properties");
                v = updateVertexProperties(v, record);
            } else {
                log.info("Vertex not found");
                throw new VertexNotFound("vertex with pk: " + vertexPk + "=" + record.get(vertexPk));
            }
        }catch (Exception e){
            throw e;
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
        log.info("Starting new thread with id: " + myID + " for jobYype: "+ jobType);
        graphTransaction = getGraph().newTransaction();
        getRecords().forEachRemaining(new Consumer<Map<String, String>>() {
            @Override
            public void accept(Map<String, String> record) {
                try {
                    switch (jobType) {
                        case Constants.POST:
                            postRecord(record);
                            break;
                        case Constants.PATCH:
                            patchRecord(record);
                            break;
                        case Constants.PUT:
                            putRecord(record);
                    }
                } catch (Exception e) {
                    log.error("Thread " + myID + ". Exception during record import.", e);
                }
            }
        });

        graphTransaction.commit();
        graphTransaction.close();

        log.info("Finished thread " + myID);

        notifyListener(WorkerListener.State.Done);
    }

}
