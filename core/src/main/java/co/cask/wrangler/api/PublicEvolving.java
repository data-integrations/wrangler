package co.cask.wrangler.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation to mark classes and methods for public use, but with evolving interfaces.
 *
 * <p>Classes and methods with this annotation are intended for public use and have stable behavior.
 * However, their interfaces and signatures are not considered to be stable and might be changed
 * across versions.
 *
 * <p>This annotation also excludes methods and classes with evolving interfaces / signatures
 * within classes annotated with {@link Public}.
 *
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR })
@Public
public @interface PublicEvolving {
}