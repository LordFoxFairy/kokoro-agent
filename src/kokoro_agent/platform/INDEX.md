---
architectureIndex: 1
rootId: agent.platform
owners:
  - "@LordFoxFairy"
---

# Platform ports

- `media.py` — private Media Runtime port. It translates GA product intent into the
  Root-owned Connect contract, performs command-journal recovery, and projects only
  model-safe operation handles.
- `memory.py` — typed Product Memory port placeholder. Durable user/profile/episodic memory
  remains a Platform-owned service boundary; DeepAgents `memory=` is limited to instruction
  files and must not become a hidden product memory store.

This package is an outbound adapter boundary. Provider selection, Site policy,
credits, storage, and media execution remain Platform-owned.
