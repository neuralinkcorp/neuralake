document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.querySelector('.md-search__input');
    if (searchInput) {
        const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
        searchInput.placeholder = isMac ? 'Search (⌘K)' : 'Search (Ctrl+K)';
    }
});

document.addEventListener('keydown', function(e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        const searchInput = document.querySelector('.md-search__input');
        if (searchInput) {
            searchInput.focus();
        }
    }
}); 