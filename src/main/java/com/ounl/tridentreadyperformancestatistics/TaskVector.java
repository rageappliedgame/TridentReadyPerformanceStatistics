/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ounl.tridentreadyperformancestatistics;

/**
 *
 * @author gla
 */
public class TaskVector {
    public int taskID;
    public int trial;
    public Double mean;
    public Double SD;
    public Long n;    
    
    TaskVector() {
        taskID = 0;
        trial = 0;
        mean = 0.0;
        SD = 0.0;
        n = 0L;
    }
}
