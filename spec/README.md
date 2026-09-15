# The profile

`profile-v1.md` is the contract every implementation in this repository speaks — and the one an
implementation elsewhere, in another language, would speak to interoperate. `profile.json` carries
its constants for tooling. `vectors/*.sse` are golden frames: exact wire bytes that every encoder
must produce and every parser must read back. A change here is a change to every implementation,
and their tests read these files in place.
