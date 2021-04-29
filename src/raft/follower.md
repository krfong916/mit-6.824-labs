follower

appendentries (receiving)

- reply args.success = false
  - if args.term < currentTerm
- update term
  - term = args.term

requestvote (receiving)

- reply args.voteGranted = false, args.term = currentTerm
  - if args.term < currentTerm
- reply args.voteGranted = true
  - if votedFor == nil
  - update votedFor, update the election timeout - (?)
