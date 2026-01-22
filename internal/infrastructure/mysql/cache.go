package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"bcindex/internal/application"
	"bcindex/internal/domain"

	"github.com/redis/go-redis/v9"
)

const (
	logCacheVersionKey = "bcindex:logs:version"
	logCacheKeyPrefix  = "bcindex:logs:v"
	defaultCacheTTL    = time.Hour
)

type CacheConfig struct {
	Addr string
	TTL  time.Duration
}

type CachedRepository struct {
	*Repository
	cache *redis.Client
	ttl   time.Duration
}

func NewCachedRepository(base *Repository, cfg CacheConfig) (*CachedRepository, error) {
	if base == nil {
		return nil, errors.New("base repository is required")
	}
	if strings.TrimSpace(cfg.Addr) == "" {
		return &CachedRepository{Repository: base}, nil
	}
	if cfg.TTL <= 0 {
		cfg.TTL = defaultCacheTTL
	}
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Addr,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &CachedRepository{Repository: base, cache: client, ttl: cfg.TTL}, nil
}

func (r *CachedRepository) StoreLogs(ctx context.Context, logs []domain.LogEntry) error {
	if err := r.Repository.StoreLogs(ctx, logs); err != nil {
		return err
	}
	if len(logs) == 0 {
		return nil
	}
	r.invalidateLogCache(ctx)
	return nil
}

func (r *CachedRepository) DeleteLogsFrom(ctx context.Context, chainID uint64, fromBlock uint64) error {
	if err := r.Repository.DeleteLogsFrom(ctx, chainID, fromBlock); err != nil {
		return err
	}
	r.invalidateLogCache(ctx)
	return nil
}

func (r *CachedRepository) QueryLogs(ctx context.Context, filter application.LogQueryFilter) ([]domain.LogEntry, error) {
	if r.cache == nil {
		return r.Repository.QueryLogs(ctx, filter)
	}
	version, ok := r.cacheVersion(ctx)
	if !ok {
		return r.Repository.QueryLogs(ctx, filter)
	}
	key := logCacheKey(version, filter)
	if cached, err := r.cache.Get(ctx, key).Result(); err == nil {
		var logs []domain.LogEntry
		if err := json.Unmarshal([]byte(cached), &logs); err == nil {
			return logs, nil
		}
	}

	logs, err := r.Repository.QueryLogs(ctx, filter)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(logs)
	if err != nil {
		return logs, nil
	}
	_ = r.cache.Set(ctx, key, payload, r.ttl).Err()
	return logs, nil
}

func (r *CachedRepository) cacheVersion(ctx context.Context) (string, bool) {
	version, err := r.cache.Get(ctx, logCacheVersionKey).Result()
	if err == nil {
		return version, true
	}
	if errors.Is(err, redis.Nil) {
		return "0", true
	}
	return "", false
}

func (r *CachedRepository) invalidateLogCache(ctx context.Context) {
	if r.cache == nil {
		return
	}
	_ = r.cache.Incr(ctx, logCacheVersionKey).Err()
}

func logCacheKey(version string, filter application.LogQueryFilter) string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString(logCacheKeyPrefix)
	b.WriteString(version)
	b.WriteString(":chain=")
	if filter.ChainID != nil {
		b.WriteString(strconv.FormatUint(*filter.ChainID, 10))
	} else {
		b.WriteString("all")
	}
	b.WriteString(":addr=")
	if filter.Address != "" {
		b.WriteString(strings.ToLower(filter.Address))
	} else {
		b.WriteString("any")
	}
	b.WriteString(":tx=")
	if filter.TxHash != "" {
		b.WriteString(filter.TxHash)
	} else {
		b.WriteString("any")
	}
	b.WriteString(":from=")
	if filter.FromBlock != nil {
		b.WriteString(strconv.FormatUint(*filter.FromBlock, 10))
	} else {
		b.WriteString("any")
	}
	b.WriteString(":to=")
	if filter.ToBlock != nil {
		b.WriteString(strconv.FormatUint(*filter.ToBlock, 10))
	} else {
		b.WriteString("any")
	}
	b.WriteString(":limit=")
	b.WriteString(strconv.Itoa(normalizeLogLimit(filter.Limit)))
	return b.String()
}

func normalizeLogLimit(limit int) int {
	if limit <= 0 || limit > 1000 {
		return 100
	}
	return limit
}
