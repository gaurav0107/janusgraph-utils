package com.ibm.janusgraph.utils.importer.Exception;

public class CustomException {

    public class VertexNotFound extends Exception {
        public VertexNotFound(String errorMessage) {
            super(errorMessage);
        }
    }

    public static class DataNotFound extends Exception {
        public DataNotFound(String errorMessage) {
            super(errorMessage);
        }
    }


}
