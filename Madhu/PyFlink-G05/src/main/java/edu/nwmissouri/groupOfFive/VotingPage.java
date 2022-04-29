package edu.nwmissouri.groupOfFive;

import java.io.Serializable;

public class VotingPage implements Serializable {

    String name = "unknown.md";
    Double rank = 1.0;
    Integer votes = 0;

    public VotingPage(String nameIn, Double rankIn, Integer votesIn){
        this.name = nameIn;
        this.rank = rankIn;
        this.votes = votesIn;
    }

    public VotingPage(String nameIn, Integer votesIn) {
        this.name = nameIn;
        this.votes = votesIn;
    }
    
    public String getName() {
        return name;
    }
    
    public Double getRank() {
        return rank;
    }
    
    public Integer getVotes() {
        return votes;
    }

    @Override
    public String toString() {
        return String.format("%s,%.5f,%s", this.name,this.rank,this.votes);
    }
    
}
