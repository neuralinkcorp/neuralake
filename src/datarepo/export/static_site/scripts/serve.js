import { createServer } from 'http';
import { readFileSync, existsSync, writeFileSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const jsonFile = process.argv[2];
const port = parseInt(process.argv[3]) || 8000;

if (!jsonFile) {
    console.error('Please provide a JSON file path as an argument');
    console.error('Usage: npm run build-and-serve -- path/to/your/file.json [port]');
    process.exit(1);
}

if (!existsSync(jsonFile)) {
    console.error(`JSON file not found: ${jsonFile}`);
    process.exit(1);
}

try {
    const jsonContent = readFileSync(jsonFile, 'utf8');
    const precompiledDir = join(__dirname, '..', 'precompiled');
    const outputPath = join(precompiledDir, 'data.json');
    
    if (!existsSync(precompiledDir)) {
        console.error('Precompiled directory not found. Please run build first.');
        process.exit(1);
    }
    
    writeFileSync(outputPath, jsonContent);
    console.log(`Copied ${jsonFile} to ${outputPath}`);
} catch (error) {
    console.error('Error copying JSON file:', error);
    process.exit(1);
}

const server = createServer((req, res) => {
    const filePath = join(__dirname, '..', 'precompiled', req.url === '/' ? 'index.html' : req.url);
    
    try {
        const content = readFileSync(filePath);
        const contentType = filePath.endsWith('.html') ? 'text/html' :
                          filePath.endsWith('.js') ? 'application/javascript' :
                          filePath.endsWith('.css') ? 'text/css' :
                          filePath.endsWith('.json') ? 'application/json' :
                          'text/plain';
        
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
    } catch (error) {
        res.writeHead(404);
        res.end('Not found');
    }
});

server.listen(port, () => {
    console.log(`Server running at http://localhost:${port}/`);
}); 