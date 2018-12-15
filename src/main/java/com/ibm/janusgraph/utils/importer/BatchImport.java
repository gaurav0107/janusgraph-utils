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
package com.ibm.janusgraph.utils.importer;

import com.ibm.janusgraph.utils.importer.util.Constants;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import com.ibm.janusgraph.utils.importer.dataloader.DataLoader;
import com.ibm.janusgraph.utils.importer.schema.SchemaLoader;

public class BatchImport {

    public static void main(String args[]) throws Exception {

        System.out.println(args.toString());
        if (null == args || args.length < 5) {
            System.err.println(
                    "Usage: BatchImport <jobType> <janusgraph-config-file> <data-files-directory> <schema.json> <data-mapping.json> [skipSchema]");
            System.exit(1);
        }
        String jobType = null;
        JanusGraph graph = JanusGraphFactory.open(args[1]);
        if (!(args.length > 5 && args[5].equals("skipSchema")))
            new SchemaLoader().loadSchema(graph, args[3]);
        switch (args[0].toUpperCase()){
            case Constants.POST:
                jobType = Constants.POST;
                break;
            case Constants.PUT:
                jobType = Constants.PUT;
                break;
            case Constants.PATCH:
                jobType = Constants.PATCH;
                break;
            case Constants.DELETE:
                jobType = Constants.DELETE;
                break;
            default:
                System.err.println(
                        "Invalid Job Type "+ args[0]);
                System.exit(1);
                break;
        }
        new DataLoader(graph).loadVertex(args[2], args[4], jobType);
        new DataLoader(graph).loadEdges(args[2], args[4], jobType);
        graph.close();
    }
}
