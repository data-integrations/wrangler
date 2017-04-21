package co.cask.wrangler.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation for marking classes as public, stable interfaces.
 *
 * <p>Classes, methods and fields with this annotation are stable across minor releases (1.0, 1.1, 1.2). In other words,
 * applications using @Public annotated classes will compile against newer versions of the same major release.
 *
 * <p>Only major releases (1.0, 2.0, 3.0) can break interfaces with this annotation.
 */
@Documented
@Target(ElementType.TYPE)
@Public
public @interface Public {}
