# Third-party notices

The [0BSD license](LICENSE) applies to original Signal Reducers code and documentation. It does not relicense dependencies, downloaded model weights, Redis, or hosted services. Release archives contain project files and synthetic routing examples; dependency distributions and checkpoint weights are installed separately.

## Python dependencies

The following licenses were checked against installed distribution metadata and included license files on 2026-10-12. Versions identify the verified locked environment. This is a notice for direct dependencies and important native/model tooling, not an exhaustive inventory of every transitive package. `uv.lock` identifies the dependency graph; each installed distribution supplies its own complete license notices.

| Distribution | Verified version | Declared license |
|---|---|---|
| [Laya](https://github.com/NandhaKishorM/laya/blob/main/LICENSE) | 0.4.3 | Apache-2.0 |
| [Pydantic](https://github.com/pydantic/pydantic) | 2.14.1 | MIT |
| [pydantic-core](https://github.com/pydantic/pydantic/tree/main/pydantic-core) | 2.50.1 | MIT |
| [HTTPX](https://github.com/encode/httpx) | 0.28.1 | BSD-3-Clause |
| [redis-py](https://github.com/redis/redis-py) | 7.4.1 | MIT |
| [PyTorch](https://github.com/pytorch/pytorch) | 2.14.1 | Apache-2.0 AND Apache-2.0 WITH LLVM-exception AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND MIT |
| [Transformers](https://github.com/huggingface/transformers) | 5.19.0 | Apache-2.0 |
| [Tokenizers](https://github.com/huggingface/tokenizers) | 0.23.3 | Apache-2.0 |
| [Hugging Face Hub](https://github.com/huggingface/huggingface_hub) | 2.2.0 | Apache-2.0 |
| [Safetensors](https://github.com/huggingface/safetensors) | 0.8.0 | Apache-2.0 |
| [NumPy](https://github.com/numpy/numpy) | 2.5.4 | BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0 |
| [pytest](https://github.com/pytest-dev/pytest) | 9.1.1 | MIT |
| [pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio) | 1.4.0 | Apache-2.0 |
| [Hatchling](https://github.com/pypa/hatch/tree/master/backend) | 1.32.4 | MIT |

Native distributions such as PyTorch and NumPy include additional component licenses. Retain the license files shipped by their upstream distributions if redistributing those dependencies. This project does not bundle them.

## Router checkpoint

The public [Laya typed-decisions model card](https://huggingface.co/convaiinnovations/laya-typed-decisions) declares Apache-2.0. Runtime selects checkpoint revision `e929ae5cf69bc34259cd2f95c9e91145b818b1f0` and downloads weights separately through Hugging Face. Check the model's own license and attribution files before redistributing weights. Its license is distinct from this project's 0BSD grant.

## Redis and hosted services

Redis is an external executable. Redis 7.2 and earlier use BSD-3-Clause; 7.4 uses RSALv2 or SSPLv1; Redis 8 and later offer RSALv2, SSPLv1, or AGPLv3. Select and comply with the terms of the Redis version you install. The Python client is a separate MIT-licensed component. [Official Redis licensing overview](https://redis.io/legal/licenses/).

OpenRouter and the hosted DREX/OpenAI models are external services. Their access, data-processing, usage, and billing terms are supplied by those providers; access to their APIs does not grant model weights under this project's license.
