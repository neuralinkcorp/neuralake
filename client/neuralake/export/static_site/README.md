# Neuralake static site generator

This NodeJS module creates a static site that may be exported by the Python catalog. 

The static site is compiled with [Vite](https://vite.dev/) and checked into the `/precompiled` directory. This allows the `neuralake` Python catalog to generate an input JSON file from a catalog defintion for generation of the static site. 

## Development
Run `npm run build` to compile the static site into javascript under the `precompiled` directory.

Copy an output JSON from `web_export.py` into `/precompiled`. For an example, you can run `docs/examples/generate_tcph_site.py`. 

Run `python -m http.server -d precompiled` to start the server.