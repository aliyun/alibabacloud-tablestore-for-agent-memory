package com.aliyun.openservices.tablestore.agent.spring;

import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import java.util.ArrayList;
import java.util.List;
import lombok.NoArgsConstructor;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;

@NoArgsConstructor
public class TestEmbedding implements EmbeddingModel {

    private final AllMiniLmL6V2QuantizedEmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        int size = request.getInstructions().size();
        List<Embedding> embeddings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            embeddings.add(new Embedding(embeddingModel.embed(request.getInstructions().get(i)).content().vector(), i));
        }
        return new EmbeddingResponse(embeddings);
    }

    @Override
    public float[] embed(Document document) {
        return embeddingModel.embed(document.getText()).content().vector();
    }
}
