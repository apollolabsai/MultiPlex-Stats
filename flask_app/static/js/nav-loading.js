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

    function hideNavigationLoading() {
        var overlay = document.getElementById('nav-loading-overlay');
        if (overlay) {
            overlay.classList.remove('is-open');
            overlay.setAttribute('aria-hidden', 'true');
        }
    }

    // Dismiss the overlay when the user navigates back/forward.
    // The pageshow event fires on bfcache restores where the DOM still
    // has the overlay visible from the previous navigation.
    window.addEventListener('pageshow', function(event) {
        if (event.persisted) {
            hideNavigationLoading();
        }
    });

    root.showNavigationLoading = showNavigationLoading;
    root.hideNavigationLoading = hideNavigationLoading;
})(window);
