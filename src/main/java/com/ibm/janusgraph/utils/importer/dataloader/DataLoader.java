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
package com.ibm.janusgraph.utils.importer.dataloader;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.ibm.janusgraph.utils.importer.util.Constants;
import com.ibm.janusgraph.utils.importer.vertex.VertexPatchWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.JanusGraph;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.janusgraph.utils.importer.edge.EdgeLoaderWorker;
import com.ibm.janusgraph.utils.importer.util.Config;
import com.ibm.janusgraph.utils.importer.util.Worker;
import com.ibm.janusgraph.utils.importer.util.WorkerPool;
import com.ibm.janusgraph.utils.importer.vertex.VertexWorker;

public class DataLoader {
    private JanusGraph graph;

    private static final Logger log = LoggerFactory.getLogger(DataLoader.class);

    public DataLoader(JanusGraph graph) {
        this.graph = graph;
    }

    public void loadVertex(String filesDirectory, String mappingFile, String jobType) throws Exception {
        loadData(filesDirectory, mappingFile, "vertexMap", (Class) VertexWorker.class, jobType);
    }

    public void loadEdges(String filesDirectory, String mappingFile, String jobType) throws Exception {
        loadData(filesDirectory, mappingFile, "edgeMap", (Class) EdgeLoaderWorker.class, jobType);
    }


    public void updateVertex(String filesDirectory, String mappingFile) throws Exception {
        loadData(filesDirectory, mappingFile, "vertexMap", (Class) VertexPatchWorker.class, Constants.POST);
    }

    public void updateEdges(String filesDirectory, String mappingFile) throws Exception {
        loadData(filesDirectory, mappingFile, "edgeMap", (Class) EdgeLoaderWorker.class, Constants.POST);
    }

    public void loadData(String filesDirectory, String mappingFile, String mapToLoad, Class<Worker> workerClass, String jobType)
            throws Exception {
        long startTime = System.nanoTime();
        log.info("Start loading data for " + mapToLoad);

        // Read the mapping json
        String mappingJson = new String(Files.readAllBytes(Paths.get(mappingFile)));
        JSONObject mapping = new JSONObject(mappingJson);

        JSONObject nodeMap;
        try {
            nodeMap = mapping.getJSONObject(mapToLoad);
            if (nodeMap == null) {
                return;
            }
        } catch (JSONException e) {
            return;
        }

        Iterator<String> keysIter = nodeMap.keys();

        int availProcessors = Config.getConfig().getWorkers();
        try (WorkerPool workers = new WorkerPool(availProcessors, availProcessors * 2)) {
            while (keysIter.hasNext()) {
                String fileName = keysIter.next();
                Map<String, Object> propMapping = new Gson().fromJson(nodeMap.getJSONObject(fileName).toString(),
                        new TypeToken<HashMap<String, Object>>() {
                        }.getType());
                new DataFileLoader(graph, workerClass).loadFile(filesDirectory + "/" + fileName, propMapping, workers, jobType);
            }
        }

        // log elapsed time in seconds
        long totalTime = (System.nanoTime() - startTime) / 1000000000;
        log.info("Loaded " + mapToLoad + " in " + totalTime + " seconds!");
    }

    /*
    public void updateData(String filesDirectory, String mappingFile, String mapToUpdate, Class<Worker> workerClass)
            throws Exception{
        long startTime = System.nanoTime();
        log.info("Start updating data for " + mapToUpdate);

        String mappingJson = new String(Files.readAllBytes(Paths.get(mappingFile)));
        JSONObject mapping = new JSONObject(mappingJson);

        JSONObject nodeMap;
        log.info(mapping.toString());
        try {
            nodeMap = mapping.getJSONObject(mapToUpdate);
            if (nodeMap == null) {
                return;
            }
        } catch (JSONException e) {
            log.error(e.toString());
            return;
        }

        log.info(nodeMap.toString());
        Iterator<String> keysIter = nodeMap.keys();

        int availProcessors = Config.getConfig().getWorkers();
        try (WorkerPool workers = new WorkerPool(availProcessors, availProcessors * 2)) {
            while (keysIter.hasNext()) {
                String fileName = keysIter.next();
                Map<String, Object> propMapping = new Gson().fromJson(nodeMap.getJSONObject(fileName).toString(),
                        new TypeToken<HashMap<String, Object>>() {
                        }.getType());
                new DataFileLoader(graph, workerClass).loadFile(filesDirectory + "/" + fileName, propMapping, workers);
            }
        }

        // log elapsed time in seconds
        long totalTime = (System.nanoTime() - startTime) / 1000000000;
        log.info("Loaded " + mapToUpdate + " in " + totalTime + " seconds!");
    }
    */
}
