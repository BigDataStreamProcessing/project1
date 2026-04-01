package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class EsperClient {
    public static void main(String[] args) {
        int noOfRecordsPerSec;
        int howLongInSec;

        if (args.length < 2) {
            noOfRecordsPerSec = 2;
            howLongInSec = 5;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        // Wyłączamy wewnętrzny zegar — czas będzie kontrolowany przez its
        config.getRuntime().getThreading().setInternalTimerEnabled(false);

        EPCompiled epCompiled = getEPCompiled(config);

        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        } catch (EPDeployException ex) {
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService()
                .getStatement(deployment.getDeploymentId(), "answer");

        resultStatement.addListener((newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

        EPStatement inputStatement = runtime.getDeploymentService()
                .getStatement(deployment.getDeploymentId(), "input");

        inputStatement.addListener((newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("I: %s%n", eventBean.getUnderlying());
            }
        });

        Faker faker = new Faker();

        // Symulowany czas startowy — odpowiada "teraz" zaokrąglonemu do sekundy
        Instant baseInstant = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        for (int sec = 0; sec < howLongInSec; sec++) {
            // its dla tej sekundy symulowanego czasu
            Instant currentInstant = baseInstant.plusSeconds(sec);
            long currentMillis = currentInstant.toEpochMilli();
            Timestamp iTimestamp = Timestamp.from(currentInstant);

            // Awansujemy wewnętrzny zegar Espera do wartości its
            runtime.getEventService().advanceTime(currentMillis);

            for (int i = 0; i < noOfRecordsPerSec; i++) {
                // ets: losowo do 60 sekund przed its — zachowanie oryginalne
                Timestamp eTimestamp = new Timestamp(
                        currentMillis - faker.number().numberBetween(0, 60) * 1000L);
                eTimestamp.setNanos(0);

                String[] ores = {"coal", "iron", "gold", "diamond", "emerald"};
                String record = Format.toJson()
                        .set("ore", () -> ores[faker.number().numberBetween(0, ores.length)])
                        .set("depth", () -> String.valueOf(faker.number().numberBetween(1, 36)))
                        .set("amount", () -> String.valueOf(faker.number().numberBetween(1, 10)))
                        .set("ets", eTimestamp::toString)
                        .set("its", iTimestamp::toString)
                        .build().generate();

                runtime.getEventService().sendEventJson(record, "MinecraftEvent");
            }
        }

        // Awansujemy czas za ostatnią sekundę — żeby okna zdążyły wygasnąć
        runtime.getEventService().advanceTime(
                baseInstant.plusSeconds(howLongInSec).toEpochMilli());
    }

    private static EPCompiled getEPCompiled(Configuration config) {
        CompilerArguments compilerArgs = new CompilerArguments(config);
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
            epCompiled = compiler.compile("""
                    @public @buseventtype create json schema
                    MinecraftEvent(ore string, depth int, amount int, ets string, its string);
                    @name('input') SELECT * FROM MinecraftEvent;
                    @name('answer') SELECT ore, depth, amount, ets, its
                    FROM MinecraftEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 3 sec)
                    """, compilerArgs);
        } catch (EPCompileException ex) {
            throw new RuntimeException(ex);
        }
        return epCompiled;
    }
}