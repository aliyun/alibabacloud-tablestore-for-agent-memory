package com.aliyun.openservices.tablestore.agent.spring;

import com.aliyun.openservices.tablestore.agent.model.Document;
import com.aliyun.openservices.tablestore.agent.model.DocumentHit;
import com.aliyun.openservices.tablestore.agent.model.Metadata;
import java.util.Map;

class Utils {

    static Document toTablestoreDocument(boolean enableMultiTenant, float[] embedding, org.springframework.ai.document.Document springAiDocument) {
        if (springAiDocument.getMedia() != null) {
            throw new UnsupportedOperationException("Media is not supported yet.");
        }
        Map<String, Object> springMetadata = springAiDocument.getMetadata();
        String documentId = springAiDocument.getId();
        String tenantId = Document.DOCUMENT_DEFAULT_TENANT_ID;
        if (enableMultiTenant) {
            Object springTenantId = springMetadata.remove(Document.DOCUMENT_TENANT_ID);
            if (!(springTenantId instanceof String)) {
                throw new IllegalArgumentException("Multi-tenant is enabled but `tenantId` is not set in the document metadata.");
            }
            tenantId = springTenantId.toString();
        }
        String text = springAiDocument.getText();
        Metadata metadata = new Metadata(springMetadata);
        return new Document(documentId, tenantId, text, embedding, metadata);
    }

    static org.springframework.ai.document.Document toSpringAIDocument(DocumentHit documentHit) {
        Document tsDocument = documentHit.getDocument();
        Double score = documentHit.getScore();
        Map<String, Object> metaData = tsDocument.getMetadata().toMap();
        String tenantId = tsDocument.getTenantId();
        if (tenantId != null && !tenantId.equals(Document.DOCUMENT_DEFAULT_TENANT_ID)) {
            metaData.put(Document.DOCUMENT_TENANT_ID, tenantId);
        }
        return org.springframework.ai.document.Document.builder()
            .id(tsDocument.getDocumentId())
            .text(tsDocument.getText())
            .metadata(metaData)
            .score(score)
            .build();

    }
}
