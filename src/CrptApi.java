import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {
    public static final String BASE_API_URL = "https://ismp.crpt.ru/api/v3";
    public static final String CREATE_RU_URL = "/lk/documents/send";
    private final TimeUnit timeUnit;
    private long timeDelay = 1L;
    private final ScheduledExecutorService executorService;
    private final Semaphore semaphore;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newHttpClient();

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must be not null");
        if (requestLimit < 1) {
            throw new IllegalArgumentException("requestLimit must be greater than 0");
        }
        semaphore = new Semaphore(requestLimit);
        executorService = new ScheduledThreadPoolExecutor(requestLimit);
    }

    public String createDocument(DocumentDto document, String signature, ProductGroup productGroup) throws InterruptedException, IOException {
        PreparedDocumentDto preparedDocDto = prepareDocument(document, signature, productGroup);
        semaphore.acquire();
        executorService.schedule(() -> semaphore.release(), timeDelay, timeUnit);
        String response = post(preparedDocDto);
        JsonNode jsonNode = mapper.readTree(response);
        return jsonNode.get("value").asText();
    }

    private PreparedDocumentDto prepareDocument(DocumentDto document, String signature, ProductGroup productGroup) {
        PreparedDocumentDto preparedDocumentDto = new PreparedDocumentDto();
        preparedDocumentDto.setDocumentFormat(DocumentFormat.MANUAL.toString());
        List<ProductDto> products = document.getProducts();
        preparedDocumentDto.setProductGroup(productGroup.toString().toLowerCase());
        for (ProductDto product : products) {
            if (product.getUitCode() == null && product.getUituCode() == null) {
                throw new IllegalArgumentException("UitCode or UituCode must not be null");
            }
        }
        try {
            String documentBase64 = Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(document));
            preparedDocumentDto.setProductDocument(documentBase64);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        preparedDocumentDto.setType(Type.LP_INTRODUCE_GOODS.toString());
        preparedDocumentDto.setSignature(signature);
        return preparedDocumentDto;
    }

    private String post(PreparedDocumentDto preparedDocDto) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_API_URL + CREATE_RU_URL + "?pg=" + preparedDocDto.getProductGroup()))
                .header("content-type", "application/json")
                .header("Authorization", "Bearer " + AuthUtils.getToken())
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(preparedDocDto), StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        int responseCode = response.statusCode();
        if (responseCode == 200 || responseCode == 201 || responseCode == 202) {
            return response.body();
        } else {
            throw new ApiError(response.body());
        }
    }
}

@Data
class PreparedDocumentDto {
    @JsonProperty("document_format")
    String documentFormat;
    @JsonProperty("product_document")
    String productDocument;
    @JsonProperty("product_group")
    String productGroup;
    String signature;
    String type;
}

@Data
class DocumentDto {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private DescriptionDto description;
    @JsonProperty("doc_id")
    private String docId;
    @JsonProperty("doc_status")
    private String docStatus;
    @JsonProperty("doc_type")
    private String docType;
    private boolean importRequest;
    @JsonProperty("owner_inn")
    private String ownerInn;
    @JsonProperty("participant_inn")
    private String participantInn;
    @JsonProperty("producer_inn")
    private String producerInn;
    @JsonProperty("production_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private LocalDateTime productionDate;
    @JsonProperty("production_type")
    private String productionType;
    @JsonProperty("reg_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private LocalDate regDate;
    @JsonProperty("reg_number")
    private String regNumber;
    private List<ProductDto> products;
}

@Data
class DescriptionDto {
    private String participantInn;
}

@Data
class ProductDto {
    @JsonProperty("certificate_document")
    private String certificateDocument;
    @JsonProperty("certificate_document_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private LocalDateTime certificateDocumentDate;
    @JsonProperty("certificate_document_number")
    private String certificateDocumentNumber;
    @JsonProperty("owner_inn")
    private String ownerInn;
    @JsonProperty("producer_inn")
    private String producerInn;
    @JsonProperty("production_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private LocalDateTime productionDate;
    @JsonProperty("tnved_code")
    private String tnvedCode;
    @JsonProperty("uit_code")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String uitCode;
    @JsonProperty("uitu_code")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String uituCode;
}

enum DocumentFormat {
    MANUAL, XML, CSV
}

enum Type {
    LP_INTRODUCE_GOODS
}

enum ProductGroup {
    CLOTHES, SHOES, TOBACCO, PERFUMERY, TIRES, ELECTRONICS, PHARMA, MILK, BICYCLE, WHEELCHAIRS
}

class AuthUtils {
    static String getToken() {
        return "testValue";
    }
}

class ApiError extends RuntimeException {
    public ApiError(String message) {
        super(message);
    }
}