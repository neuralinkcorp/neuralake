import shutil
import os

def on_page_markdown(markdown, page, config, files):
    iframe_str = """

<div align="center" style="position: relative;">
    <button onclick="window.open('examples/web_catalog/index.html', '_blank')" style="position: absolute; top: -40px; right: 10px; z-index: 1000; padding: 8px; background: #000; color: #fff; border: none; border-radius: 4px; cursor: pointer; display: flex; align-items: center; justify-content: center;">
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
            <polyline points="15 3 21 3 21 9"></polyline>
            <line x1="10" y1="14" x2="21" y2="3"></line>
        </svg>
    </button>
    <iframe src="examples/web_catalog/index.html" frameborder="0"></iframe>
    <br><br>
</div>
    """
    if page.file.src_path == "README.md":
        markdown = markdown.replace("<!-- mkdocs:iframe -->", iframe_str)
    return markdown

def on_post_build(config):
    # Get the site directory from config
    site_dir = config['site_dir']
    
    # Source and destination paths
    src_path = os.path.join('docs', 'examples', 'web_catalog', 'data.json')
    dst_path = os.path.join(site_dir, 'data.json')
    
    # Copy the file
    shutil.copy2(src_path, dst_path)
