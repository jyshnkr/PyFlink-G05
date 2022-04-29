package edu.nwmissouri.RawData;

import java.io.Serializable;
import java.util.ArrayList;

public class AbhiRankedPage implements Serializable {
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<AbhiVotingPage> voters = new ArrayList<AbhiVotingPage>();

    AbhiRankedPage(String nameIn, ArrayList<AbhiVotingPage> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }

    AbhiRankedPage(String nameIn, Double rankIn, ArrayList<AbhiVotingPage> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
        this.rank = rankIn;

    }

    public Double getRank() {
        return rank;
    }

    public ArrayList<AbhiVotingPage> getVoters() {
        return voters;
    }

    @Override
    public String toString() {

        return String.format("%s,%.5f,%s", this.name, this.rank, this.voters.toString());
    }

}
