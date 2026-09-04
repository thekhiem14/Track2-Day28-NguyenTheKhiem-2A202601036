# Ngân hàng Q&A — Day 28 Track 2

Nguyễn Thế Khiêm — 2A202601036

Dùng kèm [`demo-script.md`](demo-script.md). Câu trả lời viết ngắn để **nói**, không phải
để đọc. Mỗi câu có phần "vì sao hỏi" để bạn nhận ra ý đồ người chấm.

**Nguyên tắc số một:** cái gì chưa chạy thì nói "em chưa chạy phần đó nên em báo
`UNVERIFIED`". Người chấm phân biệt được người trung thực và người nói vống. Rubric ghi
rõ: làm giả evidence là **0 điểm** phần tương ứng.

---

## A. Bốn hàm tự viết

**1. Vì sao `event_headers` bỏ hẳn `traceparent` khi không có trace, thay vì gửi chuỗi rỗng?**

> Vì `traceparent` rỗng không phải là "không có trace" — nó là một header W3C **sai định
> dạng**. Consumer sẽ cố parse và nhận một context hỏng, làm đứt trace ở đúng ranh giới
> mà IP10 cần chứng minh. Không có header nghĩa là "bắt đầu trace mới ở đây", và đó là
> trạng thái hợp lệ cho CLI producer và cho replay.

*Vì sao hỏi:* xem bạn hiểu sự khác nhau giữa "vắng mặt" và "rỗng".

**2. Vì sao `dedupe_latest` so `(occurred_at, event_id)` chứ không chỉ `occurred_at`?**

> Hai event cùng timestamp là chuyện bình thường — seed script hay batch submit đều tạo
> ra. Nếu chỉ so timestamp thì khi hòa nhau, kết quả phụ thuộc thứ tự Kafka trả về
> partition: cùng một dữ liệu, hai lần chạy, hai kết quả khác nhau. Thêm `event_id` làm
> thứ tự trở thành **toàn phần**, nên kết quả chỉ phụ thuộc nội dung batch.

**3. Vì sao phải dedupe ở Python? Sao không để Delta MERGE tự xử lý trùng?**

> Vì `MERGE` **không** tự xử lý được. Nó **ném lỗi** khi source có hai dòng cùng khớp
> một dòng đích. Nên batch replay không chỉ tạo dữ liệu trùng — nó làm hỏng cả job. Phải
> làm cho source duy nhất **trước** khi đưa vào MERGE.

*Vì sao hỏi:* đây là câu phân loại. Người không đọc kỹ sẽ trả lời "để cho nhanh".

**4. Vì sao sắp xếp kết quả theo khóa?**

> Để đầu ra giống hệt nhau giữa các lần replay. Nhờ vậy lúc demo em có thể `diff` hai
> lần chạy để **chứng minh** idempotency, thay vì chỉ nói miệng.

**5. Vì sao `readiness_status` mặc định `mandatory=True` và `ready=False` khi thiếu khóa?**

> Readiness phải **fail closed**. Một probe mới thêm mà quên khai báo severity sẽ bị coi
> là bắt buộc và làm `/ready` đỏ — sai theo hướng an toàn, dễ phát hiện ngay. Mặc định
> ngược lại sẽ tạo ra một `/ready` xanh giả, và đó là kiểu lỗi tệ nhất ở endpoint này.

**6. Vì sao ba trạng thái? Gộp `degraded` vào `not_ready` cho đơn giản có được không?**

> Không. `/ready` quyết định gateway có rút pod khỏi vòng quay hay không. Gộp lại thì
> một Feast lạnh sẽ rút **toàn bộ** pod khỏi tải, dù hệ thống vẫn trả lời đúng bằng
> feature mặc định — tức là tự gây outage từ một sự cố bộ phận. Ngược lại, gộp
> `degraded` vào `ready` thì mất luôn tín hiệu cảnh báo sớm.

**7. Vì sao `feast_online_request` lấy danh sách feature từ `contracts.py`?**

> Để không có nguồn sự thật thứ hai. Registry và parser phản hồi đều dùng
> `FEATURE_REFS`. Nếu em viết lại danh sách ở đây, thì thêm một feature vào registry mà
> quên sửa request sẽ khiến Feast vẫn trả 200 và hệ thống **im lặng** phục vụ thiếu
> feature. Đó là loại lỗi khó thấy nhất trong cả bài.

---

## B. Idempotency và dữ liệu

**8. Chứng minh replay không tạo dữ liệu trùng như thế nào?**

> Hai bằng chứng. Một: bảng `documents` trước và sau khi gửi lại đúng lô cũ có **version
> tăng nhưng số dòng không đổi** — version tăng nghĩa là MERGE thật sự chạy, số dòng
> không đổi nghĩa là mọi bản ghi khớp vào dòng cũ. Hai: đọc thẳng Delta thấy
> `rows == distinct_keys` ở cả hai bảng. Nếu có bản sao thì `rows` phải lớn hơn.

**9. Idempotency được bảo đảm ở mấy tầng?**

> Bốn tầng dùng chung một khóa logic. Key của Kafka message chính là `idempotency_key`.
> Delta MERGE trên đúng khóa đó, sau khi `dedupe_latest` bảo đảm source duy nhất. Qdrant
> point ID là `uuid5(ID_NAMESPACE, doc_id)` nên upsert đè đúng một điểm. Feast ghi theo
> entity.

**10. Client không gửi idempotency key thì sao?**

> API tự suy ra từ hash nội dung. Nhờ vậy client không nghĩ đến retry vẫn được bảo vệ.
> Đánh đổi: hai feedback **cố ý giống hệt nhau** của cùng một người sẽ bị gộp làm một.
> Với bài toán feedback thì mất mát đó nhỏ hơn nhiều so với nhân bản dữ liệu khi mạng
> chập chờn.

**11. Vì sao ingestion trả `202` mà không phải `201`?**

> Vì lúc đó Delta, Feast, Qdrant **chưa** được cập nhật — Airflow làm việc đó sau. Trả
> `201 Created` là nói về việc chưa xảy ra. `202` cộng `event_id` là câu trả lời trung
> thực, và `event_id` chính là thứ để bám theo một bản ghi xuyên hệ thống.

---

## C. Vận hành, sự cố, độ trễ

**12. Vì sao vượt latency budget không làm request thất bại?**

> Vì giết một request chậm không làm người dùng vui hơn, nhưng làm mất dữ liệu để chẩn
> đoán. Thay vào đó nó tăng `lab28_latency_budget_exceeded_total` theo từng component.
> Đánh đổi: **bắt buộc** phải có alert trên metric đó, nếu không regression độ trễ sẽ
> trôi qua âm thầm. Em đã ghi việc thiếu alert này vào bảng production gap.

**13. Nút thắt hiệu năng của hệ thống nằm ở đâu?**

> Đo `/ready` qua gateway ở 8 và 16 worker. Ở 16 worker chỉ 15/200 request được phục vụ,
> 185 bị `429`. Nhưng API chỉ dùng 0.35% CPU và 227 MiB RAM — nghĩa là ứng dụng **không**
> phải nút thắt. Nút thắt thứ nhất là rate limit của Envoy, `max_tokens: 10`; đó là bảo
> vệ có chủ đích. Nút thắt thứ hai là `/ready` gọi 5 probe **tuần tự**, và probe vLLM
> phải đợi hết connect timeout khi endpoint chết — một dependency chết làm chậm readiness
> của cả những dependency đang sống.

**14. Percentile em đo được có dùng làm SLO cho `/ask` không?**

> Không. `/ready` cố ý chạm mọi dependency, `/ask` thì không. Ngoài ra p50 tổng hợp còn
> gây hiểu nhầm: nó chỉ ~7 ms không phải vì hệ thống nhanh mà vì phần lớn request bị từ
> chối rất nhanh bằng `429`. Phải tách theo mã trạng thái mới đọc đúng.

**15. Vì sao DAG báo `failed` trong lúc sự cố lại là chuyện tốt?**

> Vì nó cho biết online store đang tụt lại so với lakehouse. Nếu task đó nuốt lỗi và báo
> `success`, Feast sẽ âm thầm phục vụ feature cũ — và đó mới là hỏng thật sự. Dữ liệu
> không mất: `drain_kafka_into_delta` vẫn `success` trong đúng lần chạy đó, nên bản ghi
> đã nằm bền vững trong Delta; chỉ bước materialize là chưa chạy.

**16. Vì sao chọn Feast để inject mà không chọn Kafka hay Qdrant?**

> Vì Feast là cách rõ nhất để phân biệt `degraded` với `not_ready` — đúng câu hỏi mà hàm
> `readiness_status` em viết phải trả lời. Dừng Kafka thì ingestion trả 503, đó là câu
> chuyện khác.

**17. `/health` và `/ready` khác nhau chỗ nào, và vì sao không gộp?**

> `/health` là liveness: 200 bất cứ khi nào tiến trình còn phục vụ được HTTP, và **không
> bao giờ** chạm dependency. `/ready` là readiness: chỉ 200 khi mọi dependency bắt buộc
> dùng được. Nếu gộp, một Kafka chậm sẽ làm Kubernetes **restart pod** — biến một sự cố
> phụ thuộc thành một vòng lặp restart, tức là làm hỏng thêm.

---

## D. Mô hình và phát hành

**18. Rollback model mà không sửa code bằng cách nào?**

> `champion` là một **alias**, không phải số version ghim trong code. `rollback()` tìm
> version cao nhất thấp hơn version hiện tại rồi chuyển alias sang đó. Serving path đọc
> alias nên nhận thay đổi ở lần refresh kế tiếp. Không sửa code, không build lại image,
> không deploy lại.

**19. Bằng chứng của rollback là gì?**

> Là alias trong MLflow. Em nói thêm cho chính xác: contract `ModelLifecycleEvent` và
> topic `model.events` **đã được khai báo** nhưng **chưa có thành phần nào publish** —
> em kiểm chứng bằng cách đọc topic sau khi rollback, nó rỗng. Và counter
> `lab28_release_transitions_total` thì nằm trong tiến trình CLI sống vài giây nên
> Prometheus không kịp scrape. Em đã ghi cả hai vào bảng production gap.

*Vì sao hỏi:* câu này bẫy người nói theo tài liệu mà không kiểm chứng.

**20. Vì sao không có canary khi promote?**

> Đó là một gap em đã ghi nhận. Hiện `champion` đổi là 100% traffic đổi theo ngay. Cách
> đúng là traffic split ở gateway, so sánh metric online rồi mới chuyển hẳn. Ngoài ra
> cũng chưa có eval gate tự động chặn promote khi model không đạt ngưỡng.

---

## E. IP07 và tính trung thực

**21. Vì sao IP07 chưa xong / đã xong thế nào?**

*Nếu đã nối được Kaggle:*

> Endpoint là vLLM thật: `/version` trả `0.26.0`, `/v1/models` có
> `Qwen/Qwen3-4B-Instruct-2507`, và `/metrics` có metric family `vllm:`. `lab28 ready`
> trả `ready`. Một lần `/ask` chạy trọn với `degraded: false`.

*Nếu chưa nối được:*

> Máy em **có** GPU và Docker **thấy** được GPU, nhưng vLLM không chạy được. Docker
> Desktop trên Windows chạy container trong máy ảo WSL2; vLLM phát hiện WSL rồi chủ động
> tắt pinned memory theo giới hạn NVIDIA, trong khi `UvaBuffer` của 0.28 bắt buộc phải
> có UVA nên nó raise. Em kiểm chứng bằng chính image vLLM:
> `is_pin_memory_available()` và `is_uva_available()` đều `False`. Không phải lỗi VRAM.

**22. Sao không dựng một server OpenAI-compatible cho đủ 10 điểm?**

> Vì gate `probe_identity` đòi **đồng thời** `/version` của chính vLLM và metric family
> `vllm:` — một proxy không sinh được cả hai. Và vì rubric cho **0 điểm** phần đó nếu
> làm giả. Báo `UNVERIFIED` trung thực có giá trị hơn một dấu tick giả.

**23. Vì sao vLLM là dependency duy nhất không có degraded path?**

> Feast lạnh thì dùng feature mặc định. Qdrant rỗng thì trả lời không có nguồn. Nhưng
> không có model thì **không có câu trả lời nào để trả**. Bịa một câu trả lời khi LLM
> chết sẽ phá vỡ chính grounding contract mà system prompt đang bắt buộc. Nên đó là 503
> thật.

---

## F. Nền tảng và bảo mật

**24. Rate limit hiện tại có đủ cho production không?**

> Không. Nó là **local** trên từng Envoy: `max_tokens: 10` mỗi pod. Scale ra N pod thì
> hạn mức thật thành N×10. Cần global rate limit service với backend chia sẻ.

**25. Kafka hiện tại có an toàn dữ liệu không?**

> Không. `replication_factor=1` và một broker duy nhất — mất broker là mất dữ liệu chưa
> xử lý. Production cần RF≥3, `min.insync.replicas=2` và producer `acks=all`.

**26. Rollback theo GitOps hoạt động ra sao?**

> `gitops/application.yaml` ghim `targetRevision` vào một tag. Rollback là đưa tag về
> revision trước rồi để Argo CD sync. `selfHeal: true` nghĩa là sửa tay trên cluster sẽ
> bị kéo về đúng trạng thái khai báo. Em **chưa** chạy trên cluster thật nên phần
> drift/self-heal em báo `UNVERIFIED`, chỉ xác minh tĩnh bằng `validate_manifests.py` và
> `kubectl kustomize`.

**27. Audit trail có lưu nội dung câu hỏi không?**

> Không. Chỉ lưu `input_hash`, `output_hash`, độ dài và token count. Truy được "cùng một
> câu hỏi" mà không lưu dữ liệu người dùng. Đánh đổi: khi debug một câu trả lời tệ thì
> không đọc lại được câu hỏi gốc từ log.

**28. Gap lớn nhất nếu đưa hệ thống này lên production là gì?**

> Em chọn ba. Một, **không có auth** giữa các service — ai vào được network là đọc ghi
> được tất cả. Hai, **Kafka RF=1** nên mất broker là mất dữ liệu. Ba, **trace lấy mẫu
> 100%** — chi phí và dung lượng không chịu nổi ở tải thật, cần tail-based sampling giữ
> 100% lỗi và chậm rồi giảm phần còn lại. Bảng đầy đủ 20 gap nằm ở mục 3 của
> `ANSWERS.md`.

---

## G. Câu hỏi mở hay gặp cuối buổi

**29. Nếu có thêm một tuần, bạn làm gì trước?**

> Cache và chạy song song 5 probe trong `/ready`, vì hiện tại chính readiness tự tạo tải
> và một dependency chết kéo chậm cả pod. Sau đó là alert cho DLQ và cho
> `lab28_latency_budget_exceeded_total`, vì hai thứ đó đang hỏng trong im lặng.

**30. Học được gì lớn nhất từ bài này?**

> Rằng "pipeline báo SUCCESS" không phải là bằng chứng. Em có một DAG báo `success`
> nhưng `polled: 0` — không đọc được bản tin nào, vì consumer group mới cần thời gian
> rebalance lâu hơn cửa sổ poll 3 giây. Nếu chỉ nhìn màu xanh thì em đã kết luận sai.
> Bằng chứng phải là dữ liệu, version, trace ID và khả năng khôi phục.
