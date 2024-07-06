<li>For Spark application to run with JDK17, this needs to be added in VM option</li>
<code>
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
</code>
<p>Refer : <a href="https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct" target="_blank">Spark-3 breaks on Java 17</a></p>
<li>To reflect in all class while running on IDEA, you can add this config in run template as shown below</li>
<a href="https://www.imagebam.com/view/MEUJ3AO" target="_blank"><img src="https://thumbs4.imagebam.com/96/ba/d2/MEUJ3AO_t.png" alt="modify-run-template-idea.png"/></a>
