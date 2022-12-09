package cs451.LatticeAgreement;

import java.util.Objects;
import java.util.Set;

public class Decision<T> {
    public final int consensusNumber;
    public final Set<T> value;

    public Decision(int consensusNumber, Set<T> value) {
        this.consensusNumber = consensusNumber;
        this.value = value;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Decision decision = (Decision) o;
        return consensusNumber == decision.consensusNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(consensusNumber);
    }
}
