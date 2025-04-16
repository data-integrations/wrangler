"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/dunder-proto";
exports.ids = ["vendor-chunks/dunder-proto"];
exports.modules = {

/***/ "(ssr)/./node_modules/dunder-proto/get.js":
/*!******************************************!*\
  !*** ./node_modules/dunder-proto/get.js ***!
  \******************************************/
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

eval("\n\nvar callBind = __webpack_require__(/*! call-bind-apply-helpers */ \"(ssr)/./node_modules/call-bind-apply-helpers/index.js\");\nvar gOPD = __webpack_require__(/*! gopd */ \"(ssr)/./node_modules/gopd/index.js\");\n\nvar hasProtoAccessor;\ntry {\n\t// eslint-disable-next-line no-extra-parens, no-proto\n\thasProtoAccessor = /** @type {{ __proto__?: typeof Array.prototype }} */ ([]).__proto__ === Array.prototype;\n} catch (e) {\n\tif (!e || typeof e !== 'object' || !('code' in e) || e.code !== 'ERR_PROTO_ACCESS') {\n\t\tthrow e;\n\t}\n}\n\n// eslint-disable-next-line no-extra-parens\nvar desc = !!hasProtoAccessor && gOPD && gOPD(Object.prototype, /** @type {keyof typeof Object.prototype} */ ('__proto__'));\n\nvar $Object = Object;\nvar $getPrototypeOf = $Object.getPrototypeOf;\n\n/** @type {import('./get')} */\nmodule.exports = desc && typeof desc.get === 'function'\n\t? callBind([desc.get])\n\t: typeof $getPrototypeOf === 'function'\n\t\t? /** @type {import('./get')} */ function getDunder(value) {\n\t\t\t// eslint-disable-next-line eqeqeq\n\t\t\treturn $getPrototypeOf(value == null ? value : $Object(value));\n\t\t}\n\t\t: false;\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHNzcikvLi9ub2RlX21vZHVsZXMvZHVuZGVyLXByb3RvL2dldC5qcyIsIm1hcHBpbmdzIjoiQUFBYTs7QUFFYixlQUFlLG1CQUFPLENBQUMsc0ZBQXlCO0FBQ2hELFdBQVcsbUJBQU8sQ0FBQyxnREFBTTs7QUFFekI7QUFDQTtBQUNBO0FBQ0EsaUNBQWlDLHNDQUFzQztBQUN2RSxFQUFFO0FBQ0Y7QUFDQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQSwyRUFBMkUsK0JBQStCOztBQUUxRztBQUNBOztBQUVBLFdBQVcsaUJBQWlCO0FBQzVCO0FBQ0E7QUFDQTtBQUNBLGVBQWUsaUJBQWlCO0FBQ2hDO0FBQ0E7QUFDQTtBQUNBIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vZGF0YS1pbmdlc3Rpb24tdG9vbC1mcm9udGVuZC8uL25vZGVfbW9kdWxlcy9kdW5kZXItcHJvdG8vZ2V0LmpzPzZkMmIiXSwic291cmNlc0NvbnRlbnQiOlsiJ3VzZSBzdHJpY3QnO1xuXG52YXIgY2FsbEJpbmQgPSByZXF1aXJlKCdjYWxsLWJpbmQtYXBwbHktaGVscGVycycpO1xudmFyIGdPUEQgPSByZXF1aXJlKCdnb3BkJyk7XG5cbnZhciBoYXNQcm90b0FjY2Vzc29yO1xudHJ5IHtcblx0Ly8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIG5vLWV4dHJhLXBhcmVucywgbm8tcHJvdG9cblx0aGFzUHJvdG9BY2Nlc3NvciA9IC8qKiBAdHlwZSB7eyBfX3Byb3RvX18/OiB0eXBlb2YgQXJyYXkucHJvdG90eXBlIH19ICovIChbXSkuX19wcm90b19fID09PSBBcnJheS5wcm90b3R5cGU7XG59IGNhdGNoIChlKSB7XG5cdGlmICghZSB8fCB0eXBlb2YgZSAhPT0gJ29iamVjdCcgfHwgISgnY29kZScgaW4gZSkgfHwgZS5jb2RlICE9PSAnRVJSX1BST1RPX0FDQ0VTUycpIHtcblx0XHR0aHJvdyBlO1xuXHR9XG59XG5cbi8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBuby1leHRyYS1wYXJlbnNcbnZhciBkZXNjID0gISFoYXNQcm90b0FjY2Vzc29yICYmIGdPUEQgJiYgZ09QRChPYmplY3QucHJvdG90eXBlLCAvKiogQHR5cGUge2tleW9mIHR5cGVvZiBPYmplY3QucHJvdG90eXBlfSAqLyAoJ19fcHJvdG9fXycpKTtcblxudmFyICRPYmplY3QgPSBPYmplY3Q7XG52YXIgJGdldFByb3RvdHlwZU9mID0gJE9iamVjdC5nZXRQcm90b3R5cGVPZjtcblxuLyoqIEB0eXBlIHtpbXBvcnQoJy4vZ2V0Jyl9ICovXG5tb2R1bGUuZXhwb3J0cyA9IGRlc2MgJiYgdHlwZW9mIGRlc2MuZ2V0ID09PSAnZnVuY3Rpb24nXG5cdD8gY2FsbEJpbmQoW2Rlc2MuZ2V0XSlcblx0OiB0eXBlb2YgJGdldFByb3RvdHlwZU9mID09PSAnZnVuY3Rpb24nXG5cdFx0PyAvKiogQHR5cGUge2ltcG9ydCgnLi9nZXQnKX0gKi8gZnVuY3Rpb24gZ2V0RHVuZGVyKHZhbHVlKSB7XG5cdFx0XHQvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgZXFlcWVxXG5cdFx0XHRyZXR1cm4gJGdldFByb3RvdHlwZU9mKHZhbHVlID09IG51bGwgPyB2YWx1ZSA6ICRPYmplY3QodmFsdWUpKTtcblx0XHR9XG5cdFx0OiBmYWxzZTtcbiJdLCJuYW1lcyI6W10sInNvdXJjZVJvb3QiOiIifQ==\n//# sourceURL=webpack-internal:///(ssr)/./node_modules/dunder-proto/get.js\n");

/***/ })

};
;