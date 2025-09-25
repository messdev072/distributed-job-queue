package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"context"

	"github.com/google/uuid"
	"github.com/gorhill/cronexpr"
	"github.com/redis/go-redis/v9"
)

// Schedule represents a recurring job definition.
type Schedule struct {
	ID          string `json:"id"`
	Queue       string `json:"queue"`
	Payload     string `json:"payload"`
	Priority    int    `json:"priority"`
	Cron        string `json:"cron"`
	Enabled     bool   `json:"enabled"`
	LastRunUnix int64  `json:"last_run_unix"`
	NextRunUnix int64  `json:"next_run_unix"`
	CreatedUnix int64  `json:"created_unix"`
	UpdatedUnix int64  `json:"updated_unix"`
}

var (
	ErrNotFound = errors.New("schedule not found")
)

type Store struct {
	Rdb *redis.Client
	Ctx context.Context
}

func NewStore(rdb *redis.Client, ctx context.Context) *Store { return &Store{Rdb: rdb, Ctx: ctx} }

func (s *Store) key(id string) string { return fmt.Sprintf("schedules:%s", id) }

func (s *Store) Create(sc *Schedule) error {
	if sc.ID == "" {
		sc.ID = uuid.New().String()
	}
	now := time.Now().Unix()
	sc.CreatedUnix = now
	sc.UpdatedUnix = now
	if sc.Enabled {
		if err := s.computeNext(sc); err != nil {
			return err
		}
	}
	b, _ := json.Marshal(sc)
	return s.Rdb.Set(s.Ctx, s.key(sc.ID), b, 0).Err()
}

func (s *Store) Update(sc *Schedule) error {
	if sc.ID == "" {
		return errors.New("missing id")
	}
	sc.UpdatedUnix = time.Now().Unix()
	if sc.Enabled {
		if err := s.computeNext(sc); err != nil {
			return err
		}
	} else {
		sc.NextRunUnix = 0
	}
	b, _ := json.Marshal(sc)
	return s.Rdb.Set(s.Ctx, s.key(sc.ID), b, 0).Err()
}

func (s *Store) Get(id string) (*Schedule, error) {
	val, err := s.Rdb.Get(s.Ctx, s.key(id)).Result()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var sc Schedule
	if err := json.Unmarshal([]byte(val), &sc); err != nil {
		return nil, err
	}
	return &sc, nil
}

func (s *Store) Delete(id string) error { return s.Rdb.Del(s.Ctx, s.key(id)).Err() }

// List returns up to N schedules (simple scan)
func (s *Store) List(limit int) ([]*Schedule, error) {
	cursor := uint64(0)
	res := []*Schedule{}
	pattern := "schedules:*"
	for {
		keys, cur, err := s.Rdb.Scan(s.Ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			val, err := s.Rdb.Get(s.Ctx, k).Result()
			if err != nil {
				continue
			}
			var sc Schedule
			if json.Unmarshal([]byte(val), &sc) == nil {
				res = append(res, &sc)
				if limit > 0 && len(res) >= limit {
					return res, nil
				}
			}
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	return res, nil
}

func (s *Store) computeNext(sc *Schedule) error {
	if sc.Cron == "" {
		return errors.New("missing cron expression")
	}
	expr, err := cronexpr.Parse(sc.Cron)
	if err != nil {
		return err
	}
	next := expr.Next(time.Now())
	sc.NextRunUnix = next.Unix()
	return nil
}

// Due returns enabled schedules whose NextRunUnix <= now
func (s *Store) Due(now time.Time, limit int) ([]*Schedule, error) {
	all, err := s.List(0)
	if err != nil {
		return nil, err
	}
	due := []*Schedule{}
	n := now.Unix()
	for _, sc := range all {
		if sc.Enabled && sc.NextRunUnix > 0 && sc.NextRunUnix <= n {
			due = append(due, sc)
			if limit > 0 && len(due) >= limit {
				break
			}
		}
	}
	return due, nil
}

// MarkRun updates LastRun and NextRun after executing a schedule.
func (s *Store) MarkRun(sc *Schedule) error {
	sc.LastRunUnix = time.Now().Unix()
	// compute next
	if err := s.computeNext(sc); err != nil {
		return err
	}
	return s.Update(sc)
}
