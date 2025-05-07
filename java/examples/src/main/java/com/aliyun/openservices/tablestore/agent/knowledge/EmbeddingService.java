package com.aliyun.openservices.tablestore.agent.knowledge;

import ai.djl.Application;
import ai.djl.Device;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.Artifact;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.util.Assert;

@Slf4j
public class EmbeddingService implements EmbeddingModel, Closeable {

    private final String modelName;
    private final ZooModel<String, float[]> model;

    /*
     * 比较火的 Embedding 本地模型 “BAAI/bge-base-zh-v1.5”为例, 维度768，使用djl可直接本地基于cpu跑起来。 模型细节：https://modelscope.cn/models/BAAI/bge-base-zh-v1.5
     */
    public static final String DEFAULT_MODEL_NAME = "ai.djl.huggingface.rust/BAAI/bge-base-zh-v1.5/0.0.1/bge-base-zh-v1.5";

    public EmbeddingService() throws Exception {
        this(DEFAULT_MODEL_NAME);
    }

    public EmbeddingService(String modelName) throws Exception {
        this.modelName = modelName;
        log.info("use embedding model:{}", modelName);
        Criteria<String, float[]> criteria = Criteria.builder()
            .setTypes(String.class, float[].class)
            .optModelUrls("djl://" + modelName)
            .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
            .optProgress(new ProgressBar())
            .build();
        this.model = criteria.loadModel();
    }

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        int size = request.getInstructions().size();
        List<Embedding> embeddings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            embeddings.add(new Embedding(innerEmbed(request.getInstructions().get(i)), i));
        }
        return new EmbeddingResponse(embeddings);
    }

    private float[] innerEmbed(String text) {
        try (Predictor<String, float[]> predictor = model.newPredictor()) {
            return predictor.predict(text);
        } catch (Exception e) {
            throw new RuntimeException(String.format("embed text error: %s", text), e);
        }
    }

    @Override
    public float[] embed(Document document) {
        Assert.notNull(document.getText(), "Document text must not be null");
        return innerEmbed(document.getText());
    }

    /**
     * 列出支持的模型
     */
    public List<String> listModels() throws ModelNotFoundException, IOException {
        Criteria<?, ?> criteria = Criteria.builder()
            .optApplication(Application.NLP.TEXT_EMBEDDING)
            .optDevice(Device.cpu())
            .optProgress(new ProgressBar())
            .build();
        Map<Application, List<Artifact>> models = ModelZoo.listModels(criteria);
        return models.values().stream().flatMap(Collection::stream).map(artifact -> {
            StringBuilder sb = new StringBuilder(100);
            var metadata = artifact.getMetadata();
            if (metadata != null) {
                sb.append(metadata.getGroupId()).append('/').append(metadata.getArtifactId()).append('/');
            }
            if (artifact.getVersion() != null) {
                sb.append(artifact.getVersion()).append('/');
            }
            sb.append(artifact.getName());
            return sb.toString();
        }).toList();
    }

    @Override
    public void close() throws IOException {
        log.info("closing embedding model:{}", modelName);
        if (model != null) {
            model.close();
        }
    }
}
