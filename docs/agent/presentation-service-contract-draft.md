# AgentPresentationService Root contract delta

This is the Agent child-owner input for the Root Proto/Buf authority. It is deliberately not a local
handwritten cross-repository wire contract.

## Services

```proto
service AgentPresentationService {
  rpc PullCandidateBatches(PullCandidateBatchesRequest) returns (PullCandidateBatchesResponse);
  rpc AcknowledgeCandidateAdmissions(AcknowledgeCandidateAdmissionsRequest)
      returns (PresentationDeliveryStatus);
  rpc QuarantineCandidateAdmission(QuarantineCandidateAdmissionRequest)
      returns (PresentationDeliveryStatus);
  rpc GetDeliveryStatus(GetPresentationDeliveryStatusRequest)
      returns (PresentationDeliveryStatus);
}
```

`PullCandidateBatchesRequest` requires `run_id`, `after_presentation_seq`, `page_size` and optional
`snapshot_through_presentation_seq`. The first response freezes and returns the snapshot head; every
later page must repeat it. A record contains:

- `presentation_ref`, `presentation_seq` and the full
  `kokoro-agent-agui-candidate.v1` envelope bytes;
- `envelope_sha256`, `recorded_at`, `producer_instance_ref` and `producer_generation`.

The response contains ordered records, `snapshot_through_presentation_seq`, optional
`next_after_presentation_seq`, `has_more`, and a delivery-status snapshot.

`AcknowledgeCandidateAdmissionsRequest` requires an idempotency ref, expected contiguous ACK
watermark, one-or-more sequential typed receipts, and a domain-separated request/effect digest. Each
receipt binds presentation ref/sequence, candidate ref, Session admission receipt ref and Session
effect digest. Reusing the idempotency ref with a different digest is a conflict.

`QuarantineCandidateAdmissionRequest` is the only permanent-rejection path. It must target exactly
`acknowledged_through + 1`, bind the presentation/candidate identities and Session rejection digest,
and does not advance the ACK watermark. Later ACKs are rejected until an explicit future operator
resolution contract clears/replaces the quarantine.

`PresentationDeliveryStatus` contains run id, acknowledged-through sequence, revision, and optional
quarantined sequence/reason. All mutations use expected-watermark CAS. Pull never implies ACK and no
Agent GC is authorized by a pull response.
