# Several EventLog objects on ONE path: `run!` builds a fresh one per call, so concurrent masters
# in a single process hold several objects pointing at the same file.

using SweepRunner, Test, JSON3, Serialization

@testset "EventLog: objects on one path share a lock" begin
    dir = mktempdir()
    try
        p = joinpath(dir, "e.jsonl")
        a, b = EventLog(p), EventLog(p)
        @test a.lock === b.lock
        # A different path must NOT share it, or the lock is just a global mutex.
        @test EventLog(joinpath(dir, "other.jsonl")).lock !== a.lock
        # The path is normalised, so a relative spelling of the same file still shares.
        @test EventLog(relpath(p, pwd())).lock === a.lock
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "EventLog: a deserialized log serialises against its siblings" begin
    # `run!` sends the EventLog to every worker, and deserialization skips the constructor, so the
    # `lock` field that arrives there is private. log_event resolves the lock from the path.
    dir = mktempdir()
    try
        p = joinpath(dir, "e.jsonl")
        a = EventLog(p)
        io = IOBuffer()
        serialize(io, a)
        seekstart(io)
        b = deserialize(io)
        @test b.lock !== a.lock                    # the field really does not survive

        n = 200
        Threads.@sync begin
            Threads.@spawn for j in 1:n
                log_event(a, :key_acquired; key="a$(j)_" * "x"^40, acq="ok")
            end
            Threads.@spawn for j in 1:n
                log_event(b, :key_acquired; key="b$(j)_" * "x"^40, acq="ok")
            end
        end
        lines = readlines(p)
        @test length(lines) == 2 * n
        @test length([JSON3.read(l) for l in lines]) == 2 * n
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "EventLog: concurrent writers on one path lose and tear nothing" begin
    # Before the shared lock this lost about a fifth of the events outright and left the rest
    # spliced into each other, so the assertion is on COUNT as well as on parseability.
    dir = mktempdir()
    try
        p = joinpath(dir, "e.jsonl")
        logs = [EventLog(p) for _ in 1:4]
        n = 200
        Threads.@sync for (i, lg) in enumerate(logs)
            Threads.@spawn for j in 1:n
                log_event(
                    lg, :key_acquired; stage=:s, key="m$(i)_k$(j)_" * "x"^40, acq="ok"
                )
            end
        end

        lines = readlines(p)
        @test length(lines) == 4 * n
        recs = [JSON3.read(l) for l in lines]          # throws on a torn line
        @test length(recs) == 4 * n
        @test all(r -> r.kind == "key_acquired", recs)
        @test length(unique(String(r.key) for r in recs)) == 4 * n
    finally
        rm(dir; recursive=true, force=true)
    end
end

@testset "EventLog: separate processes on one path do not tear either" begin
    # The cross-process half, which no lock can cover: it is the single unbuffered `write` plus
    # POSIX O_APPEND. Four short-lived processes, one file.
    dir = mktempdir()
    try
        p = joinpath(dir, "e.jsonl")
        project = dirname(Base.active_project())
        script = joinpath(dir, "w.jl")
        write(
            script,
            """
            using SweepRunner
            lg = EventLog(ARGS[1])
            for j in 1:150
                log_event(lg, :key_acquired; stage=:s, key="p\$(ARGS[2])_k\$(j)_" * "x"^40, acq="ok")
            end
            """,
        )
        procs = [
            run(
                pipeline(
                    `$(Base.julia_cmd()) --project=$project $script $p $i`;
                    stdout=devnull,
                    stderr=devnull,
                );
                wait=false,
            ) for i in 1:4
        ]
        foreach(wait, procs)

        lines = readlines(p)
        @test length(lines) == 4 * 150
        @test length([JSON3.read(l) for l in lines]) == 4 * 150
    finally
        rm(dir; recursive=true, force=true)
    end
end
