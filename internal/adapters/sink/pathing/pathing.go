package pathing

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"landing-connector/internal/config"
	"landing-connector/internal/model"
)

func BuildFilePath(basePath string, filePrefix string, window model.BatchWindow, extension string) string {
	start := window.StartedAt.UTC()
	partitions := make([]int, 0, len(window.OffsetsByPart))
	for partition := range window.OffsetsByPart {
		partitions = append(partitions, int(partition))
	}
	sort.Ints(partitions)

	var suffixParts []string
	for _, partition := range partitions {
		offsets := window.OffsetsByPart[int32(partition)]
		suffixParts = append(suffixParts, fmt.Sprintf("p%02d-%d", partition, offsets.StartOffset))
	}

	fileName := fmt.Sprintf(
		"%s-%s-%s.%s",
		filePrefix,
		window.RunID,
		strings.Join(suffixParts, "_"),
		extension,
	)

	return path.Join(
		basePath,
		fmt.Sprintf("dt=%s", start.Format("2006-01-02")),
		fmt.Sprintf("hr=%02d", start.Hour()),
		fileName,
	)
}

func FilePrefix(cfg config.Config) string {
	if cfg.Output.FilePrefix == "" {
		return "part"
	}
	return cfg.Output.FilePrefix
}
