// Debounce function for performance optimization
export function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

// Throttle function for performance optimization
export function throttle(func, limit) {
  let inThrottle;
  return function executedFunction(...args) {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, limit);
    }
  };
}

// Memoization helper
export function memoize(fn) {
  const cache = new Map();
  return function (...args) {
    const key = JSON.stringify(args);
    if (cache.has(key)) {
      return cache.get(key);
    }
    const result = fn.apply(this, args);
    cache.set(key, result);
    return result;
  };
}

// Lazy loading helper
export function lazyLoad(importFn) {
  return React.lazy(() => importFn());
}

// Virtual scrolling helper
export class VirtualScroller {
  constructor(container, itemHeight, items) {
    this.container = container;
    this.itemHeight = itemHeight;
    this.items = items;
    this.visibleItems = Math.ceil(container.clientHeight / itemHeight);
    this.startIndex = 0;
    this.endIndex = this.visibleItems;
  }

  getVisibleItems() {
    return this.items.slice(this.startIndex, this.endIndex);
  }

  updateScroll(scrollTop) {
    this.startIndex = Math.floor(scrollTop / this.itemHeight);
    this.endIndex = this.startIndex + this.visibleItems;
  }
}

// Image optimization helper
export function optimizeImage(url, width, height, quality = 80) {
  // Add image optimization parameters to URL
  const params = new URLSearchParams();
  params.append('w', width);
  params.append('h', height);
  params.append('q', quality);
  return `${url}?${params.toString()}`;
}

// Resource preloading helper
export function preloadResources(resources) {
  resources.forEach(resource => {
    if (resource.endsWith('.js')) {
      const script = document.createElement('script');
      script.src = resource;
      script.async = true;
      document.head.appendChild(script);
    } else if (resource.endsWith('.css')) {
      const link = document.createElement('link');
      link.rel = 'preload';
      link.href = resource;
      link.as = 'style';
      document.head.appendChild(link);
    }
  });
}

// Performance monitoring helper
export class PerformanceMonitor {
  constructor() {
    this.metrics = new Map();
  }

  startMeasure(name) {
    this.metrics.set(name, performance.now());
  }

  endMeasure(name) {
    const startTime = this.metrics.get(name);
    if (startTime) {
      const duration = performance.now() - startTime;
      this.metrics.delete(name);
      return duration;
    }
    return null;
  }

  getMetrics() {
    return Object.fromEntries(this.metrics);
  }
} 