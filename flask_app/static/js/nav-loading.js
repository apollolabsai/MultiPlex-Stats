/**
 * Shared navigation loading overlay utility.
 *
 * Usage:
 *   showNavigationLoading('Movie Title');
 */
(function(root) {
    'use strict';

    function showNavigationLoading(titleText) {
        var overlay = document.getElementById('nav-loading-overlay');
        var titleEl = document.getElementById('nav-loading-title');
        var subtitleEl = document.getElementById('nav-loading-subtitle');
        if (!overlay || !titleEl || !subtitleEl) {
            return;
        }

        titleEl.textContent = 'Loading content details...';
        subtitleEl.textContent = titleText || 'Please wait';
        overlay.classList.add('is-open');
        overlay.setAttribute('aria-hidden', 'false');
    }

    root.showNavigationLoading = showNavigationLoading;
})(window);
