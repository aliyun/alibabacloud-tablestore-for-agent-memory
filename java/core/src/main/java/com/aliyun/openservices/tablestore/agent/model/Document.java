package com.aliyun.openservices.tablestore.agent.model;

import com.aliyun.openservices.tablestore.agent.util.TablestoreHelper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

@ToString
@Data
@Accessors(chain = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode
public class Document {

    /**
     * Document ID
     */
    private final String documentId;

    /**
     * Tenant ID.
     *
     * <p>
     * The tenant ID for multi-tenant scenarios; it can be unused if there's no multi-tenancy involved.
     * </p>
     * <p>
     * In multi-tenant scenarios, this field can be used where tenants could be knowledge bases, users, organizations, etc., depending on the
     * business scenario.
     * </p>
     * <p>
     * Typically, using user IDs or knowledge base IDs as tenant IDs provides generality.
     * </p>
     */
    private String tenantId;

    /**
     * Document Text
     */
    private String text;

    /**
     * Document Vector
     */
    @ToString.Exclude
    private float[] embedding;

    /**
     * Document Metadata
     */
    private Metadata metadata;

    public Document(Document document) {
        this(document.getDocumentId(), document.getTenantId(), document.getText(), document.getEmbedding(), document.getMetadata());
    }

    public Document(String documentId) {
        this(documentId, DOCUMENT_DEFAULT_TENANT_ID);
    }

    public Document(String documentId, String tenantId) {
        this(documentId, tenantId, null, null, new Metadata());
    }

    public Document(String documentId, String tenantId, String text, float[] embedding, Metadata metadata) {
        this.documentId = documentId;
        this.tenantId = tenantId;
        this.text = text;
        this.embedding = embedding;
        this.metadata = metadata;
    }

    @ToString.Include(name = "embedding")
    private String shortEmbedding() {
        return TablestoreHelper.maxOrNull(embedding == null ? null : TablestoreHelper.encodeEmbedding(embedding), 80, "]");
    }

    /**
     * When multi-tenant capability is not enabled, use this fixed value.
     */
    public static final String DOCUMENT_DEFAULT_TENANT_ID = "__default";
    /**
     * Document ID Field Name
     */
    public static final String DOCUMENT_DOCUMENT_ID = "document_id";
    /**
     * Tenant ID Field Name
     */
    public static final String DOCUMENT_TENANT_ID = "tenant_id";
}
