package io.cdap.wrangler.parser;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.wrangler.registry.DirectiveScope;
import org.junit.Assert;
import org.junit.Test;

public class DirectiveClassTest {

    @Test
    public void testDirectiveClassCreation() {
        // Arrange
        String name = "testDirective";
        String className = "TestDirectiveClass";
        DirectiveScope scope = DirectiveScope.USER;
        // Update the ArtifactId constructor as per your API
        ArtifactId artifactId = new ArtifactId("testArtifact:1.0.0"); // Adjust this line based on the correct constructor
        String byteSizeArg = "10KB";
        String timeDurationArg = "150ms";

        // Act
        DirectiveClass directiveClass = new DirectiveClass(name, className, scope, artifactId, byteSizeArg, timeDurationArg);

        // Assert
        Assert.assertEquals(name, directiveClass.getName());
        Assert.assertEquals(className, directiveClass.getClassName());
        Assert.assertEquals(scope, directiveClass.getScope());
        Assert.assertEquals(artifactId, directiveClass.getArtifactId());
        Assert.assertEquals(byteSizeArg, directiveClass.getByteSizeArg());
        Assert.assertEquals(timeDurationArg, directiveClass.getTimeDurationArg());
    }

    @Test
    public void testDirectiveClassToString() {
        // Arrange
        String name = "testDirective";
        String className = "TestDirectiveClass";
        DirectiveScope scope = DirectiveScope.USER;
        ArtifactId artifactId = new ArtifactId("testArtifact:1.0.0"); // Adjust this line based on the correct constructor
        String byteSizeArg = "10KB";
        String timeDurationArg = "150ms";

        // Act
        DirectiveClass directiveClass = new DirectiveClass(name, className, scope, artifactId, byteSizeArg, timeDurationArg);
        String expectedString = "DirectiveClass{name='testDirective', scope=USER, artifactId=testArtifact:1.0.0, className='TestDirectiveClass', byteSizeArg='10KB', timeDurationArg='150ms'}";

        // Assert
        Assert.assertEquals(expectedString, directiveClass.toString());
    }
}