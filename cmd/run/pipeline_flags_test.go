package run

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

func parsePipelineConfig(t *testing.T, args ...string) *serverconfig.Config {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	util.PrepareTempConfigDir(t)

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return nil
	}
	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs(append([]string{"run"}, args...))
	require.NoError(t, rootCmd.Execute())

	cfg, err := ReadConfig()
	require.NoError(t, err)
	return cfg
}

func TestListObjectsPipelineKnobs(t *testing.T) {
	t.Run("defaults_match_upstream_pipeline_and_schema", func(t *testing.T) {
		cfg := parsePipelineConfig(t)
		require.Equal(t, 100, cfg.ListObjectsPipelineChunkSize)
		require.Equal(t, 128, cfg.ListObjectsPipelineBufferCapacity)
		require.Equal(t, 3, cfg.ListObjectsPipelineNumProcs)

		_, basepath, _, _ := runtime.Caller(0)
		schema, err := os.ReadFile(path.Join(filepath.Dir(basepath), "..", "..", ".config-schema.json"))
		require.NoError(t, err)
		res := gjson.ParseBytes(schema)
		for key, want := range map[string]int{
			"listObjectsPipelineChunkSize":      cfg.ListObjectsPipelineChunkSize,
			"listObjectsPipelineBufferCapacity": cfg.ListObjectsPipelineBufferCapacity,
			"listObjectsPipelineNumProcs":       cfg.ListObjectsPipelineNumProcs,
		} {
			val := res.Get("properties." + key + ".default")
			require.True(t, val.Exists(), key)
			require.EqualValues(t, want, val.Int(), key)
		}
	})

	t.Run("flags", func(t *testing.T) {
		cfg := parsePipelineConfig(t,
			"--listObjects-pipeline-chunk-size", "1000",
			"--listObjects-pipeline-buffer-capacity", "16",
			"--listObjects-pipeline-num-procs", "8",
		)
		require.Equal(t, 1000, cfg.ListObjectsPipelineChunkSize)
		require.Equal(t, 16, cfg.ListObjectsPipelineBufferCapacity)
		require.Equal(t, 8, cfg.ListObjectsPipelineNumProcs)
	})

	t.Run("env", func(t *testing.T) {
		t.Setenv("OPENFGA_LIST_OBJECTS_PIPELINE_CHUNK_SIZE", "500")
		t.Setenv("OPENFGA_LIST_OBJECTS_PIPELINE_BUFFER_CAPACITY", "32")
		t.Setenv("OPENFGA_LIST_OBJECTS_PIPELINE_NUM_PROCS", "4")
		cfg := parsePipelineConfig(t)
		require.Equal(t, 500, cfg.ListObjectsPipelineChunkSize)
		require.Equal(t, 32, cfg.ListObjectsPipelineBufferCapacity)
		require.Equal(t, 4, cfg.ListObjectsPipelineNumProcs)
	})

	t.Run("invalid_values_are_rejected", func(t *testing.T) {
		for _, mutate := range []func(*serverconfig.Config){
			func(c *serverconfig.Config) { c.ListObjectsPipelineChunkSize = 0 },
			func(c *serverconfig.Config) { c.ListObjectsPipelineNumProcs = 0 },
			func(c *serverconfig.Config) { c.ListObjectsPipelineBufferCapacity = -1 },
		} {
			cfg := serverconfig.DefaultConfig()
			mutate(cfg)
			require.Error(t, cfg.VerifyServerSettings())
		}
	})
}
