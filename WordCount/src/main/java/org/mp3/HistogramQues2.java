package org.mp3;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class HistogramQues2 extends Application {

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        // Set the file path where the output file is located
        String outputFilePath = "/Users/niharchauhan/Downloads/part-00000-ques2";

        try (BufferedReader reader = new BufferedReader(new FileReader(outputFilePath))) {
            String documentRowString;

            // TreeMap to store the hourly tweet count for creating a histogram graph
            TreeMap<Integer, Integer> histogramGraph = new TreeMap<>();

            // Read each line from the file and process the data
            while ((documentRowString = reader.readLine()) != null) {

                // Split the line into divisions based on the tab character
                String[] divisions = documentRowString.split("\t");

                // Parse the hour and aggregate values from the divisions
                int hour = Integer.parseInt(divisions[0]);
                int aggregate = Integer.parseInt(divisions[1]);

                // Store the hour and aggregate in the TreeMap
                histogramGraph.put(hour, aggregate);
            }

            // Set up the JavaFX BarChart
            CategoryAxis xAxisLine = new CategoryAxis();
            NumberAxis yAxisLine = new NumberAxis();

            BarChart<String, Number> chart = new BarChart<>(xAxisLine, yAxisLine);

            // Create a series for the graph data
            XYChart.Series<String, Number> graphOccurring = new XYChart.Series<>();

            // Populate the graph series with data from the TreeMap
            for (Map.Entry<Integer, Integer> entry : histogramGraph.entrySet()) {
                XYChart.Data<String, Number> data = new XYChart.Data<>(String.valueOf(entry.getKey()), entry.getValue());
                graphOccurring.getData().add(data);
            }

            // Add the graph series to the chart
            chart.getData().add(graphOccurring);

            // Set up the JavaFX Scene
            Scene scene = new Scene(chart, 900, 900);
            primaryStage.setScene(scene);

            // Set the title for the application window
            primaryStage.setTitle("Hourly Tweet Count");

            // Display the application window
            primaryStage.show();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
