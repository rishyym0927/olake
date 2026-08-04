package abstract

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (a *AbstractDriver) Backfill(mainCtx context.Context, backfilledStreams chan string, pool *destination.WriterPool, stream types.StreamInterface) error {
	chunksSet := a.state.GetChunks(stream.Self())
	var err error
	if chunksSet == nil || chunksSet.Len() == 0 {
		stopPlan := logger.TrackTiming(stream.ID(), "chunk planning")
		chunksSet, err = a.driver.GetOrSplitChunks(mainCtx, pool, stream)
		stopPlan()
		if err != nil {
			return fmt.Errorf("failed to get or split chunks: %s", err)
		}
		// set state chunks
		a.state.SetChunks(stream.Self(), chunksSet)
	}
	chunks := chunksSet.Array()
	if len(chunks) == 0 {
		if backfilledStreams != nil {
			backfilledStreams <- stream.ID()
		}
		return nil
	}

	// Sort chunks by their minimum value
	sort.Slice(chunks, func(i, j int) bool {
		return typeutils.Compare(chunks[i].Min, chunks[j].Min) < 0
	})

	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	filterDataBySelectedColumnsFn := stream.RetainSelectedColumns()

	chunkProcessor := func(gCtx context.Context, _ int, chunk types.Chunk) (err error) {
		// create backfill context, so that main context not affected if backfill retries
		backfillCtx, backfillCtxCancel := context.WithCancel(gCtx)
		defer backfillCtxCancel()

		threadID := generateThreadID(stream.ID(), fmt.Sprintf("min[%v]-max[%v]", chunk.Min, chunk.Max))
		// Declared before the cleanup defers below so it fires after them: the span covers
		// the writer commit as well, not just the read loop.
		defer logger.TrackTiming(threadID, "chunk total")()
		stopWriter := logger.TrackTiming(threadID, "writer setup")
		inserter, prevMetadataState, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID), destination.WithApplyFilter(slices.Contains(constants.FullRefreshPostReadFilterDrivers, constants.DriverType(a.driver.Type()))))
		stopWriter()
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}

		defer func(ctx context.Context) {
			if ctx.Err() != nil {
				return
			}
			chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
			if chunksLeft == 0 && backfilledStreams != nil {
				backfilledStreams <- stream.ID()
			}
			logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
		}(backfillCtx)

		defer handleWriterCleanup(backfillCtx, backfillCtxCancel, &err, inserter, threadID, nil, nil)

		if prevMetadataState != nil {
			if slices.Contains(prevMetadataState.FullRefreshCommittedIDs, threadID) {
				logger.Infof("Thread[%s]: chunk min[%v] and max[%v] already committed, skipping", threadID, chunk.Min, chunk.Max)
				return nil
			}
		}

		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())

		stopIterate := logger.TrackTiming(threadID, "chunk iterate")
		defer stopIterate()
		return a.driver.ChunkIterator(backfillCtx, stream, chunk, func(ctx context.Context, data map[string]any, sourceBytes int64) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			olakeColumns := map[string]any{
				constants.OlakeID:        olakeID,
				constants.OpType:         "r",
				constants.OlakeTimestamp: time.Now().UTC(),
			}

			// Add CDC specific columns only for CDC mode
			if stream.GetSyncMode() == types.CDC {
				olakeColumns[constants.CdcTimestamp] = time.Unix(0, 0)
			}

			filteredData := filterDataBySelectedColumnsFn(data)

			return inserter.Push(ctx, types.CreateRawRecord(filteredData, olakeColumns), sourceBytes)
		})
	}
	utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, chunks, a.driver.MaxRetries(), chunkProcessor)
	return nil
}
