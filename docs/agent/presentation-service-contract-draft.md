# PresentationService Root R1 child implementation note

Root owns the Proto/Buf authority. Agent implements the generated contract and does not maintain a
second handwritten wire schema.

## Services

```proto
service PresentationService {
  rpc CheckActive(CheckActiveRequest) returns (CheckActiveResponse);
  rpc PullRecords(PullRecordsRequest) returns (PullRecordsResponse);
  rpc AcknowledgeAdmissions(AcknowledgeAdmissionsRequest)
      returns (AcknowledgeAdmissionsResponse);
  rpc QuarantineSubmission(QuarantineSubmissionRequest)
      returns (QuarantineSubmissionResponse);
  rpc GetDeliveryStatus(GetDeliveryStatusRequest)
      returns (GetDeliveryStatusResponse);
}
```

`PullRecordsRequest` requires `run_id`, producer fence, `after_delivery_seq`, and `page_size`; the
optional `snapshot_through_delivery_seq` freezes the first response head across later pages. Each
`DeliveryRecord` carries:

- `record_ref`, contiguous `delivery_seq`, and its predecessor coordinates;
- exact canonical Root R1 `PresentationSubmission` `envelope_bytes` plus `envelope_digest`;
- `submission_ref`, `submission_digest`, producer fence, recorded time, and record-chain digest.

The Agent persists canonical Submission bytes during append. Pull reads the serialized record that
was committed with those bytes; it does not reconstruct an envelope.

`AcknowledgeAdmissionsRequest` advances only a contiguous prefix under expected status/watermark
CAS. Each receipt binds record and Submission identities plus Session's admission receipt/effect.
Reusing an idempotency identity with different bytes is a conflict.

`QuarantineSubmissionRequest` is the only permanent-rejection path. It targets exactly the first
unacknowledged delivery sequence, binds record/Submission identities and Session's rejection digest,
and never advances the ACK watermark. Pull never implies ACK or Agent garbage collection.
