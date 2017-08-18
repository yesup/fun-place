package com.yesup.script;

import com.yesup.as.AddRecord;
import com.yesup.fun.Constants;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import netscape.javascript.JSObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 18/08/17.
 */
public class HelloWorld {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(AddRecord.class);
    }

    static class FilterAllClass implements ClassFilter {
        @Override
        public boolean exposeToScripts(String s) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String code1 = "var counter = 1; var getCounter = function() { counter = counter + 1; return counter; };";
        String code2 = "var counter = 1; var getCounter = function() { counter = counter + 2; return counter; };";

        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();

        ScriptEngine engine = factory.getScriptEngine(new FilterAllClass());

        ScriptContext ctx1 = new SimpleScriptContext();
        ctx1.setBindings(engine.createBindings(), ScriptContext.ENGINE_SCOPE);
        engine.eval(code1, ctx1);

        ScriptObjectMirror som1 = (ScriptObjectMirror)ctx1.getAttribute("getCounter", ScriptContext.ENGINE_SCOPE);
        Double counter = (Double)som1.call(new Object());

        LOG.info("ctx1 counter {}", counter);

        ScriptContext ctx2 = new SimpleScriptContext();
        ctx2.setBindings(engine.createBindings(), ScriptContext.ENGINE_SCOPE);
        engine.eval(code2, ctx2);

        ScriptObjectMirror som2 = (ScriptObjectMirror)ctx2.getAttribute("getCounter", ScriptContext.ENGINE_SCOPE);
        counter = (Double)som2.call(new Object());

        LOG.info("ctx2 counter {}", counter);

        counter = (Double)som2.call(new Object());

        LOG.info("ctx2 counter {}", counter);
    }
}
