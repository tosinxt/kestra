package io.kestra.core.runners;

import java.nio.file.Path;
import java.util.Optional;

import io.kestra.core.runners.configuration.TasksConfiguration;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Factory class for the constructing new {@link WorkingDir} objects.
 */
@Singleton
public class WorkingDirFactory {

    @Inject
    protected TasksConfiguration tasksConfiguration;

    /**
     * Creates a new {@link WorkingDir} instance.
     *
     * @return The {@link WorkingDir}.
     */
    public WorkingDir createWorkingDirectory() {
        return new LocalWorkingDir(getTmpDir());
    }

    /**
     * Creates the {@link WorkingDir} shared by all the tasks of a WorkingDirectory on this worker.
     *
     * @param workingDirectoryId The id of the task run of the WorkingDirectory.
     * @return The {@link WorkingDir}, always the same folder for the same id.
     */
    public WorkingDir createWorkingDirectory(String workingDirectoryId) {
        return new LocalWorkingDir(getTmpDir(), "working-directory-" + workingDirectoryId);
    }

    private Path getTmpDir() {
        return Optional.ofNullable(tasksConfiguration.tmpDir())
            .map(TasksConfiguration.TmpDir::path)
            .map(Path::of)
            .orElse(Path.of(System.getProperty("java.io.tmpdir")));
    }
}
