candidate

appendentries (receives, from someone claiming to be leader)

- reply args.success = false
  - if args.term < currentTerm
- convertToFollower()
  - if args.term >= currentTerm
  - update the election timeout

requestvote (receiving) safety from another candidate requesting vote

- reply args.voteGranted = false, args.term = currentTerm
  - if args.term < currentTerm
- reply args.voteGranted = false
  - if votedFor != nil

requestVote (reply)

- update count
  - if reaching the node was successful and the vote was granted
