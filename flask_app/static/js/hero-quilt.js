/**
 * Shared hero quilt poster background utility.
 *
 * Usage:
 *   var hero = new HeroQuilt({
 *       backdropId: 'dashboard-hero-backdrop',
 *       heroId: 'dashboard-hero',
 *       tileClass: 'dashboard-hero-tile',
 *       rows: 3,
 *       minHeight: 186,
 *       fetchUrl: '/api/dashboard/top-posters'
 *   });
 *   hero.load();                       // fetch posters from API
 *   window.addEventListener('resize', function() { hero.scheduleRerender(); });
 */
(function(root) {
    'use strict';

    function HeroQuilt(options) {
        this.backdropEl = document.getElementById(options.backdropId);
        this.heroEl = document.getElementById(options.heroId);
        this.tileClass = options.tileClass || 'hero-tile';
        this.rows = options.rows || 2;
        this.gap = options.gap || 2;
        this.overflowColumns = options.overflowColumns || 3;
        this.minHeight = options.minHeight || 124;
        this.fetchUrl = options.fetchUrl || '';
        this.posters = [];
        this._resizeTimer = null;
    }

    HeroQuilt.prototype.calculateLayout = function() {
        if (!this.backdropEl || !this.heroEl) {
            return null;
        }

        var heroHeight = Math.max(this.minHeight, this.heroEl.clientHeight || 0);
        var heroWidth = Math.max(320, this.heroEl.clientWidth || 0);
        var tileHeight = Math.max(60, Math.floor((heroHeight - this.gap * (this.rows - 1)) / this.rows));
        var tileWidth = Math.max(40, Math.round(tileHeight * (2 / 3)));
        var columns = Math.max(1, Math.ceil(heroWidth / (tileWidth + this.gap)) + this.overflowColumns);

        return {
            rows: this.rows,
            gap: this.gap,
            tileHeight: tileHeight,
            tileWidth: tileWidth,
            columns: columns,
            tileCount: columns * this.rows
        };
    };

    HeroQuilt.prototype.render = function(posters) {
        if (!this.backdropEl) {
            return;
        }

        this.backdropEl.innerHTML = '';
        if (!Array.isArray(posters) || posters.length === 0) {
            return;
        }

        var normalizedPosters = posters.filter(function(item) {
            return item && item.poster_url;
        });
        if (normalizedPosters.length === 0) {
            return;
        }

        var layout = this.calculateLayout();
        if (!layout) {
            return;
        }

        this.backdropEl.style.gridTemplateRows = 'repeat(' + layout.rows + ', ' + layout.tileHeight + 'px)';
        this.backdropEl.style.gridAutoColumns = layout.tileWidth + 'px';
        this.backdropEl.style.rowGap = layout.gap + 'px';
        this.backdropEl.style.columnGap = layout.gap + 'px';

        var tileClass = this.tileClass;
        for (var i = 0; i < layout.tileCount; i++) {
            var item = normalizedPosters[i % normalizedPosters.length];
            var posterUrl = String(item.poster_url || '').replace(/"/g, '\\"');
            var tile = document.createElement('div');
            tile.className = tileClass;
            tile.style.backgroundImage = 'url("' + posterUrl + '")';
            if (item.title) {
                tile.setAttribute('title', item.title);
            }
            this.backdropEl.appendChild(tile);
        }
    };

    HeroQuilt.prototype.load = function(callback) {
        if (!this.fetchUrl) {
            return;
        }
        var self = this;
        fetch(this.fetchUrl)
            .then(function(response) {
                if (!response.ok) {
                    throw new Error('Failed to load hero posters.');
                }
                return response.json();
            })
            .then(function(payload) {
                self.posters = Array.isArray(payload.posters) ? payload.posters : [];
                self.render(self.posters);
                if (callback) {
                    callback(self.posters);
                }
            })
            .catch(function(error) {
                console.error('Error loading hero posters:', error);
            });
    };

    HeroQuilt.prototype.scheduleRerender = function() {
        if (!this.posters.length) {
            return;
        }
        var self = this;
        if (this._resizeTimer) {
            clearTimeout(this._resizeTimer);
        }
        this._resizeTimer = setTimeout(function() {
            self.render(self.posters);
        }, 120);
    };

    root.HeroQuilt = HeroQuilt;
})(window);
