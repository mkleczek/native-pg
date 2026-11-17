# Rules for agents

## Work in jj changes, never in git-style linear history
- **jj changes, not a history of commits.** Never build up a linear history the way git work does.
- **A jj change is a unit of work:** a feature, a fix, or one part of a larger piece of work.
- **A jj change is not immutable.** When the work changes, change its jj change, for example with `jj squash --into` or `jj describe`. Don't add fix-up changes on top. Architecture changes are the exception: see "Changing the architecture".

## Dependencies follow the work
- **Split larger work into parts, each in its own jj change.**
- **A change's parents show what it depends on.** A change sits on another only if its work needs that work.
- **Independent parts are siblings.** If two parts of the work don't depend on each other, their jj changes have the same parent.

## The shape of larger work
Each change points to its parents:

```
initial rearchitecting change
  <- baseline architecture: an empty change with a bookmark
       <- work streams: sibling changes, each a chain if it has parts
            <- grouping change of a feature: one merge of its streams
                 <- product grouping change: one merge of the features' grouping changes
```

1. **Start with an enabling change.** It rearchitects or restructures the code so that the later parts can proceed in parallel, for example with a new API whose implementation does nothing yet.
2. **Put an empty change on it, with a bookmark that names it as the baseline architecture.** Work streams start from the baseline, never from the rearchitecting change itself.
3. **Build the parts in parallel,** as sibling streams of jj changes on the baseline.
4. **Join each feature's streams in one grouping change:** a single merge whose parents are the heads of the streams.
5. **Grouping changes may form a shallow hierarchy.** For example, feature1, feature2 and feature3 are sibling grouping changes, and one product grouping change has the three as its parents.

## Changing the architecture
- **Don't edit a rearchitecting change.** Make each update to the architecture a new jj change, and insert it as a parent of the baseline architecture change. The work streams follow the baseline; then adjust them to the new architecture in their own changes.
  - For an update that builds on the current architecture: `jj new --insert-after <architecture change> --insert-before <baseline> -m "…"`.
  - For an update that is independent of it, add it as another parent of the baseline: `jj rebase -s <baseline> -d <current parents> -d <new change>`.
- **Why:** the history then keeps the path to the right architecture, so it can be analysed later and the process improved.

## The grouping merge has no conflicts
- **A conflict means two streams are not independent.** Never resolve it in the merge.
- **First, consider changing the architecture.** Before rebuilding one stream on another, see whether a new rearchitecting change, inserted as a parent of the baseline, removes the overlap. The conflict is a chance to find the right architecture.
- **Otherwise, rebuild the dependent stream on the stream it depends on,** and resolve the conflict in that stream's own changes.
- **Check before merging.** Make a throwaway merge with `jj new --no-edit A B C -m trial`, list its conflicts with `jj resolve --list -r <trial>`, then `jj abandon` it.
