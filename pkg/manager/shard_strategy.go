package manager

import (
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type ShardStrategy interface {
	UpdatePodSpec(pod *corev1.PodSpec, replica int, replicaCount int) error
}

type ConsistentShardStrategy struct {
	populationSize int
}

func (c *ConsistentShardStrategy) UpdatePodSpec(pod *corev1.PodSpec, replica int, replicaCount int) error {
	// find flytepropeller pod


	// TODO hamersaw - implement
	return nil
}

type LoadBalancingShardStrategy struct {
}

func (l *LoadBalancingShardStrategy) UpdatePodSpec(pod *corev1.PodSpec, replica int, replicaCount int) error {
	return errors.New("unimplemented")
}

type ProjectDomainShardStrategy struct {
}

func (p *ProjectDomainShardStrategy) UpdatePodSpec(pod *corev1.PodSpec, replica int, replicaCount int) error {
	return errors.New("unimplemented")
}
