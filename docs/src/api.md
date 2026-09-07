# API reference

```@docs
SweepRunner
```

## AtomicIO

```@docs
SweepRunner.atomic_write
SweepRunner.atomic_touch
```

## EventLog

```@docs
SweepRunner.EventLog
SweepRunner.log_event
SweepRunner.merge_event_logs
```

## Manifest

```@docs
SweepRunner.Manifest
SweepRunner.manifest_path
SweepRunner.load_manifest
SweepRunner.save_manifest
SweepRunner.add_complete!
SweepRunner.is_complete
SweepRunner.todo_keys
SweepRunner.manifest_root
SweepRunner.merge_and_save_manifest!
```

## InitWorkers

```@docs
SweepRunner.init_workers!
SweepRunner.detect_mode
SweepRunner.verify_workers!
```

## Run

```@docs
SweepRunner.RunOpts
SweepRunner.run!
SweepRunner.run_loop!
```
