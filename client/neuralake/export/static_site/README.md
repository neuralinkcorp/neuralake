# Neuralake static site generator

This NodeJS module creates a static site that may be exported by the Python catalog. 

The static site is compiled with [Vite](https://vite.dev/) into the `/precompiled` directory. The precompiled static site expects a JSON config, named `data.json` at the root directory.

## Development
Ensure you have a JSON Neuralake catalog definition. This can be exported with `export_neuralake` in `web_export.py`. Then run the following command to generate the precompiled assets and copy over your data.json file.

### Using default port 8000
npm run build-and-serve -- path/to/your/file.json

### Specifying a custom port
npm run build-and-serve -- path/to/your/file.json 3000