package cs324.assignment2.worker;

import cs324.assignment2.grpc.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Worker-owned, fenced election configuration. The lowest registered ID
 * serializes round proposals; candidacy reports independently choose the winner.
 * Configuration changes require every current voter and retain the previous
 * recovered configuration so recovery cannot forget its replica quorum. */
final class WorkerElectionMetadata {
    private final WorkerNode node;
    private final Object state = new Object(), proposer = new Object();
    private ClusterView view = ClusterView.getDefaultInstance();
    private ClusterView lastReady = ClusterView.getDefaultInstance();
    private long promised, expires, activated;

    WorkerElectionMetadata(WorkerNode node) { this.node = node; }
    private List<WorkerInfo> members() {
        return node.getActiveWorkersFromBootstrap().stream().sorted(Comparator.comparingInt(WorkerInfo::getId)).toList();
    }
    ClusterView current() {
        List<WorkerInfo> active = members();
        synchronized (state) {
            if (!view.getVotersList().equals(active)
                    || (view.getElectionInProgress() && System.nanoTime() > expires))
                return view.toBuilder().clearCoordinator().setReady(false).setElectionInProgress(false).build();
            if (view.getReady() && view.getCoordinator().getId() == node.getMyId() && activated != view.getEpoch())
                return view.toBuilder().setReady(false).setElectionInProgress(true).build();
            return view;
        }
    }
    ElectionState localState(boolean accepted) {
        synchronized (state) {
            return ElectionState.newBuilder().setCurrent(view).setLastReady(lastReady)
                    .setPromisedEpoch(promised).setAccepted(accepted).build();
        }
    }
    ClusterView begin(BeginElectionRequest request) {
        List<WorkerInfo> active = members();
        if (active.isEmpty()) return current();
        WorkerInfo owner = active.getFirst();
        if (owner.getId() != node.getMyId()) return node.peer(owner).beginElection(request);
        synchronized (proposer) {
            ClusterView current = current();
            if (current.getElectionInProgress() || (current.getReady() && !request.getForce())
                    || request.getObservedEpoch() < current.getEpoch()) return current;
            long highest = current.getEpoch();
            ClusterView previous = localState(false).getLastReady();
            for (WorkerInfo worker : active) try {
                ElectionState answer = worker.getId() == node.getMyId() ? localState(false)
                        : node.peer(worker).getElectionState(GetClusterViewRequest.getDefaultInstance());
                highest = Math.max(highest, Math.max(answer.getPromisedEpoch(), answer.getCurrent().getEpoch()));
                if (answer.getLastReady().getEpoch() > previous.getEpoch()) previous = answer.getLastReady();
            } catch (RuntimeException ignored) { /* Cannot complete preparation until the member replies or expires. */ }
            ClusterView proposed = ClusterView.newBuilder().setEpoch(highest + 1).setMembershipVersion(highest + 1)
                    .setInitiator(owner).addAllVoters(active).addAllPreviousVoters(previous.getVotersList())
                    .setElectionInProgress(true).build();
            broadcast(proposed, previous, ViewPhase.PREPARE);
            return current();
        }
    }
    ElectionState vote(ViewVoteRequest request) {
        ClusterView proposed = request.getView();
        List<WorkerInfo> active = members();
        synchronized (state) {
            if (active.isEmpty() || !proposed.getVotersList().equals(active)
                    || !proposed.getInitiator().equals(active.getFirst()) || proposed.getEpoch() < promised)
                return localState(false);
            if (request.getPhase() == ViewPhase.PREPARE) {
                if (proposed.hasCoordinator() || proposed.getReady() || !proposed.getElectionInProgress()) return localState(false);
                if (proposed.getEpoch() == promised) {
                    return localState(sameRound(proposed, view));
                }
                ClusterView previous = request.getPreviousReady();
                if (!proposed.getPreviousVotersList().equals(previous.getVotersList())
                        || previous.getEpoch() >= proposed.getEpoch()
                        || previous.getEpoch() < lastReady.getEpoch()) return localState(false);
                promised = proposed.getEpoch(); view = proposed;
                if (previous.getEpoch() > lastReady.getEpoch()) lastReady = previous;
                expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
                return localState(true);
            }
            if (proposed.getEpoch() != promised || !sameRound(proposed, view)
                    || !proposed.hasCoordinator() || !active.contains(proposed.getCoordinator())
                    || (view.hasCoordinator() && !view.getCoordinator().equals(proposed.getCoordinator()))) return localState(false);
            if (request.getPhase() == ViewPhase.SELECTED) {
                // A repeated selection must not undo a completed readiness vote.
                if (!view.getReady()) view = proposed.toBuilder().setReady(false).setElectionInProgress(true).build();
            } else if (request.getPhase() == ViewPhase.RECOVERED) {
                if (!view.hasCoordinator()) return localState(false);
                view = proposed.toBuilder().setReady(true).setElectionInProgress(false).build();
                lastReady = view;
            } else return localState(false);
            return localState(true);
        }
    }
    private static boolean sameRound(ClusterView a, ClusterView b) {
        return a.getEpoch() == b.getEpoch() && a.getInitiator().equals(b.getInitiator())
                && a.getVotersList().equals(b.getVotersList()) && a.getPreviousVotersList().equals(b.getPreviousVotersList());
    }
    private boolean broadcast(ClusterView proposed, ClusterView previous, ViewPhase phase) {
        ViewVoteRequest request = ViewVoteRequest.newBuilder().setView(proposed).setPreviousReady(previous).setPhase(phase).build();
        boolean accepted = true;
        for (WorkerInfo worker : proposed.getVotersList()) try {
            ElectionState answer = worker.getId() == node.getMyId() ? vote(request) : node.peer(worker).voteView(request);
            accepted &= answer.getAccepted();
        } catch (RuntimeException error) { accepted = false; }
        return accepted;
    }
    ClusterView publish(PublishCoordinatorRequest request) {
        ClusterView target = current();
        if (target.getEpoch() != request.getEpoch() || target.getInitiator().getId() != node.getMyId()
                || request.getInitiatorId() != node.getMyId() || !target.getElectionInProgress()) return target;
        WorkerInfo winner = target.getVotersList().stream().filter(w -> w.getId() == request.getCoordinatorId()).findFirst().orElseThrow();
        target = target.toBuilder().setCoordinator(winner).build();
        broadcast(target, localState(false).getLastReady(), ViewPhase.SELECTED);
        return current();
    }
    ClusterView ready(MarkCoordinatorReadyRequest request) {
        ClusterView target = current();
        if (target.getEpoch() != request.getEpoch() || !target.hasCoordinator()
                || target.getCoordinator().getId() != node.getMyId() || request.getCoordinatorId() != node.getMyId()) return target;
        ClusterView previous = localState(false).getLastReady();
        if (broadcast(target, previous, ViewPhase.SELECTED) && broadcast(target, previous, ViewPhase.RECOVERED)) {
            synchronized (state) { if (view.getEpoch() == target.getEpoch()) activated = target.getEpoch(); }
        }
        return current();
    }
}
