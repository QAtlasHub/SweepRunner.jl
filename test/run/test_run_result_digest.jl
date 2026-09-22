# The `.done` marker names the bytes run! wrote: the digest DataVault.save! took before its rename.

using SweepRunner, Test, DataVault, ParamIO, SHA

const _DIGEST_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _done_fields(v, key)
    pairs = (split(l, '='; limit=2) for l in eachline(DataVault._done_file(v, key)))
    return Dict(String(p[1]) => String(p[2]) for p in pairs)
end

@testset "run!: every .done names the digest of the file it wrote" begin
    outdir = mktempdir()
    try
        v = DataVault.Vault(_DIGEST_CFG; run="digest", outdir=outdir)
        keys = ParamIO.expand(v.spec)
        res = run!(key -> Dict{String,Any}("N" => key.params["N"]), v, keys)
        @test res.done == length(keys)
        for key in keys
            f = _done_fields(v, key)
            @test f["done_version"] == "2"
            @test f["result_sha256"] ==
                bytes2hex(open(sha256, DataVault._data_file(v, key)))
            @test joinpath(outdir, f["result_file"]) == DataVault._data_file(v, key)
        end
    finally
        rm(outdir; recursive=true, force=true)
    end
end
