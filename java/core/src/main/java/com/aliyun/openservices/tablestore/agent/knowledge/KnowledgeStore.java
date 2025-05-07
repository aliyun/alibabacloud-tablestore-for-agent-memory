package com.aliyun.openservices.tablestore.agent.knowledge;

import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.DocumentHit;
import com.aliyun.openservices.tablestore.agent.model.Response;
import com.aliyun.openservices.tablestore.agent.model.filter.Filter;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public interface KnowledgeStore {

    /**
     * Insert document
     *
     * @param document document
     */
    void putDocument(Document document);

    /**
     * Update document
     *
     * @param document document
     */
    void updateDocument(Document document);

    /**
     * Delete a single document
     *
     * @param documentId Document ID
     * @param tenantId   Tenant ID (if multi-tenancy capability is not used, pass empty value)
     */
    void deleteDocument(String documentId, String tenantId);

    /**
     * Delete a single document (For non-multi-tenant scenarios only; use {@link KnowledgeStore#deleteDocument(String, String)} for
     * multi-tenant)
     *
     * @param documentId Document ID
     */
    void deleteDocument(String documentId);

    /**
     * Delete documents based on tenant.
     *
     * @param tenantId Tenant ID
     */
    void deleteDocumentByTenant(String tenantId);

    /**
     * Delete documents based on tenant id and metadata filter.
     *
     * @param tenantIds      Tenant ID (if multi-tenancy capability is not used, pass empty value)
     * @param metadataFilter Metadata filter condition
     */
    void deleteDocument(Set<String> tenantIds, Filter metadataFilter);

    /**
     * Delete all documents
     */
    void deleteAllDocuments();

    /**
     * Get a single document
     *
     * @param documentId Document ID
     * @param tenantId   Tenant ID (if multi-tenancy capability is not used, pass empty value)
     * @return Document
     */
    Document getDocument(String documentId, String tenantId);

    /**
     * Get a single document (Only for non-multi-tenant scenarios; use {@link KnowledgeStore#getDocument(String, String)} for multi-tenant)
     *
     * @param documentId Document ID
     * @return Document
     */
    Document getDocument(String documentId);

    /**
     * Get multiple documents
     *
     * @param documentIdList List of document IDs
     * @param tenantId       Tenant ID (pass an empty value if multi-tenancy capability is not used)
     * @return List of documents
     */
    List<Document> getDocuments(List<String> documentIdList, String tenantId);

    /**
     * Get multiple documents (Only for non-multi-tenant scenarios; use {@link KnowledgeStore#getDocuments(List, String)} for multi-tenant)
     *
     * @param documentIdList List of document IDs
     * @return List of documents
     */
    List<Document> getDocuments(List<String> documentIdList);

    /**
     * Get all documents
     *
     * @return List of documents
     */
    Iterator<Document> listAllDocuments();

    /**
     * Search documents
     *
     * @param searchRequest Search request parameters
     * @return Search results
     */
    Response<DocumentHit> searchDocuments(KnowledgeSearchRequest searchRequest);

    /**
     * Query the text content of Document using full-text search.
     *
     * @param query          Search term
     * @param tenantIds      Tenant ID (pass an empty value if multi-tenancy capability is not used)
     * @param limit          Number of returned results
     * @param metadataFilter Filtering condition
     * @param nextToken      Pagination token. Pass the token from the previous query result to continue pagination.
     * @param columnsToGet   Fields to return
     * @return Search results
     */
    Response<DocumentHit> fullTextSearch(String query, Set<String> tenantIds, int limit, Filter metadataFilter, String nextToken, List<String> columnsToGet);

    /**
     * Query the embedding vector content of Document through vector retrieval.
     *
     * @param queryVector    Search vector
     * @param topK           Top K for vector query
     * @param minScore       min score for document in search result. If null, no filtering will be performed
     * @param tenantIds      Tenant ID (pass an empty value if multi-tenancy capability is not used)
     * @param metadataFilter Metadata filter condition
     * @param columnsToGet   Fields to return
     * @return Search results
     */
    Response<DocumentHit> vectorSearch(float[] queryVector, int topK, Float minScore, Set<String> tenantIds, Filter metadataFilter, List<String> columnsToGet);

    /**
     * Whether to enable multi-tenant
     *
     * @return true if enable multi-tenant
     */
    boolean enableMultiTenant();

    /**
     * Initialize table
     */
    void initTable();

    /**
     * Delete table and index
     */
    void deleteTableAndIndex();
}
