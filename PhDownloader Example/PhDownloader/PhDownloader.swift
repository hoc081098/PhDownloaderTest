//
//  PhDownloader.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay
import RealmSwift
import RxRealm
import RxAlamofire
import Alamofire

// MARK: - PhDownloader

public protocol PhDownloader {
  func observeState(by identifier: String) -> Observable<PhDownloadState>

  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String

  var downloadResult$: Observable<PhDownloadResult> { get }

  func enqueue(_ request: PhDownloadRequest) -> Completable

  func cancel(by identifier: String) -> Completable

  func cancelAll() -> Completable
}

public enum PhDownloaderError: Error {
  case destroyed
  case downloadError(Error)
}

public typealias PhDownloadResult = Swift.Result<PhDownloadRequest, PhDownloaderError>

public enum PhDownloaderFactory {
  public static func downloader(with options: PhDownloaderOptions) throws -> PhDownloader {
    let dataSource = PhDownloadRealLocalDataSource(realm: try realm())
    return RealSwiftyDownloader(
      options: options,
      dataSource: dataSource
    )
  }
}

// MARK: - PhDownloadLocalDataSource

protocol PhDownloadLocalDataSource {
  func update(id: String, state: PhDownloadState) throws

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) throws

  func getResults(by ids: Set<String>) -> Results<PhDownloadTaskEntity>

  func getResults(by id: String) -> Results<PhDownloadTaskEntity>

  func get(by id: String) -> PhDownloadTaskEntity?
}

extension FileManager {
  static func phDownloaderDirectory() throws -> URL {
    let url = FileManager.default
      .urls(for: .documentDirectory, in: .userDomainMask)
      .first!
      .appendingPathComponent("phDownloader", isDirectory: true)

    if !FileManager.default.fileExists(atPath: url.path) {
      try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    }

    return url
  }
}

func realm() throws -> Realm {
  let fileURL = try FileManager.phDownloaderDirectory().appendingPathComponent("default.realm")
  print(fileURL)

  Realm.Configuration.defaultConfiguration = Realm.Configuration(
    fileURL: fileURL,
    // Set the new schema version. This must be greater than the previously used
    // version (if you've never set a schema version before, the version is 0).
    schemaVersion: 1,

    // Set the block which will be called automatically when opening a Realm with
    // a schema version lower than the one set above
    migrationBlock: { migration, oldSchemaVersion in
      // We haven’t migrated anything yet, so oldSchemaVersion == 0
      if (oldSchemaVersion < 1) {
        // Nothing to do!
        // Realm will automatically detect new properties and removed properties
        // And will update the schema on disk automatically
      }
    }
  )

  return try Realm()
}

class PhDownloadRealLocalDataSource: PhDownloadLocalDataSource {
  private let realm: Realm

  init(realm: Realm) {
    self.realm = realm
  }

  func update(id: String, state: PhDownloadState) throws {
    guard let task = self.find(by: id) else {
      return
    }

    try self.realm.write {
      task.state = state.toInt
      task.updatedAt = .init()
    }
  }

  func insertOrUpdate(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: PhDownloadState
  ) throws {
    try self.realm.write {
      self.realm.add(
        PhDownloadTaskEntity(
          identifier: identifier,
          url: url,
          fileName: fileName,
          savedDir: savedDir,
          state: state.toInt
        ),
        update: .modified
      )
    }
  }

  func getResults(by ids: Set<String>) -> Results<PhDownloadTaskEntity> {
    self.realm
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier IN %@", ids)
  }

  func getResults(by id: String) -> Results<PhDownloadTaskEntity> {
    self.realm
      .objects(PhDownloadTaskEntity.self)
      .filter("SELF.identifier = %@", id)
  }

  func get(by id: String) -> PhDownloadTaskEntity? {
    self.find(by: id)
  }

  private func find(by id: String) -> PhDownloadTaskEntity? {
    self.realm.object(ofType: PhDownloadTaskEntity.self, forPrimaryKey: id)
  }
}

class PhDownloadTaskEntity: Object {
  @objc dynamic var identifier: String = ""
  @objc dynamic var url: String = ""
  @objc dynamic var fileName: String = ""
  @objc dynamic var savedDir: String = ""
  @objc dynamic var state: Int = undefined
  @objc dynamic var updatedAt: Date = .init()

  override class func primaryKey() -> String? { "identifier" }

  // MARK: Integer constants as download state

  static let undefined = -2
  static let enqueued = -1
  static let completed = 101
  static let failed = 102
  static let cancelled = 103

  convenience init(
    identifier: String,
    url: URL,
    fileName: String,
    savedDir: URL,
    state: Int
  ) {
    self.init()
    self.identifier = identifier
    self.url = url.absoluteString
    self.fileName = fileName
    self.savedDir = savedDir.path
    self.state = state
  }
}

extension PhDownloadState {
  var toInt: Int {
    switch self {
    case .undefined:
      return PhDownloadTaskEntity.undefined
    case .enqueued:
      return PhDownloadTaskEntity.enqueued
    case .downloading(let progress):
      return progress
    case .completed:
      return PhDownloadTaskEntity.completed
    case .failed:
      return PhDownloadTaskEntity.failed
    case .cancelled:
      return PhDownloadTaskEntity.cancelled
    }
  }
}

extension Int {
  var toDownloadState: PhDownloadState {
    switch self {
    case PhDownloadTaskEntity.undefined:
      return .undefined
    case PhDownloadTaskEntity.enqueued:
      return .enqueued
    case 0...100:
      return .downloading(progress: self)
    case PhDownloadTaskEntity.completed:
      return .completed
    case PhDownloadTaskEntity.failed:
      return .failed
    case PhDownloadTaskEntity.cancelled:
      return .cancelled
    default:
      fatalError("Invalid state: \(self)")
    }
  }
}

// MARK: - RealSwiftyDownloader

class RealSwiftyDownloader: PhDownloader {
  private let options: PhDownloaderOptions
  private let dataSource: PhDownloadLocalDataSource

  private let requestS = PublishRelay<PhDownloadRequest>()
  private let downloadResultS = PublishRelay<PhDownloadResult>()
  private let disposeBag = DisposeBag()

  private let throttleScheduler = ConcurrentDispatchQueueScheduler(qos: .utility)
  private var mainScheduler: MainScheduler { MainScheduler.instance }
  private var concurrentMainScheduler: ConcurrentMainScheduler { ConcurrentMainScheduler.instance }

  internal init(options: PhDownloaderOptions, dataSource: PhDownloadLocalDataSource) {
    self.options = options
    self.dataSource = dataSource

    self
      .requestS
      .map { [weak self] request in self?.downloadRequest(request) ?? .empty() }
      .merge(maxConcurrent: options.maxConcurrent)
      .flatMap { [weak self] tuple in self?.downloadInternal(tuple) ?? .empty() }
      .subscribe(onNext: { [dataSource] tuple in
        try? dataSource.update(
          id: tuple.request.identifier,
          state: tuple.state
        )
      })
      .disposed(by: self.disposeBag)
  }

  private func downloadRequest(_ request: PhDownloadRequest) -> Observable<(DownloadRequest, PhDownloadRequest)> {
    Observable
      .deferred { () -> Observable<DownloadRequest> in
        let urlRequest = URLRequest(url: request.url)
        let destination: DownloadRequest.Destination = { (temporaryURL, response) in
          (
            request.savedDir.appendingPathComponent(request.fileName),
            [.createIntermediateDirectories, .removePreviousFile]
          )
        }
        return RxAlamofire.download(urlRequest, to: destination)
      }
      .map { ($0, request) }
  }

  private func downloadInternal(_ tuple: (DownloadRequest, PhDownloadRequest)) -> Observable<(request: PhDownloadRequest, state: PhDownloadState)> {
    tuple.0.rx
      .progress()
      .throttle(self.options.throttleProgress, latest: true, scheduler: self.throttleScheduler)
      .distinctUntilChanged()
      .materialize()
      .observeOn(self.mainScheduler)
      .map { [downloadResultS] notification -> PhDownloadState in
        switch notification {

        case .next(let progress):
          if progress.totalBytes <= 0 { return .downloading(progress: 0) }
          let percent = Double(progress.bytesWritten) / Double(progress.totalBytes)
          print(100 * percent)
          return .downloading(progress: Int(100 * percent))

        case .error(let error):
          downloadResultS.accept(.failure(.downloadError(error)))
          return .failed

        case .completed:
          downloadResultS.accept(.success(tuple.1))
          return .completed
        }
      }
      .map { (tuple.1, $0) }
  }

  // MARK: Comforms `PhDownloader`

  func observeState(by identifier: String) -> Observable<PhDownloadState> {
    Observable
      .deferred { [weak self] () -> Observable<PhDownloadState> in
        guard let self = self else {
          return .error(PhDownloaderError.destroyed)
        }

        if let task = self.dataSource.get(by: identifier) {
          return Observable
            .from(object: task, emitInitialValue: true)
            .map { $0.state.toDownloadState }
            .subscribeOn(self.concurrentMainScheduler)
        }

        return Observable
          .collection(
            from: self.dataSource.getResults(by: identifier),
            synchronousStart: true,
            on: .main
          )
          .map { results in results.first?.state.toDownloadState ?? .undefined }
          .subscribeOn(self.concurrentMainScheduler)
      }
      .distinctUntilChanged()
  }

  func observeState<T: Sequence>(by identifiers: T) -> Observable<[String: PhDownloadState]> where T.Element == String {
    Observable
      .collection(
        from: self.dataSource.getResults(by: Set(identifiers)),
        synchronousStart: true,
        on: .main
      )
      .map { results in
        let tuples = Array(results).map { ($0.identifier, $0.state.toDownloadState) }
        let stateById = Dictionary(uniqueKeysWithValues: tuples)
        return Dictionary(uniqueKeysWithValues: identifiers.map { ($0, stateById[$0] ?? .undefined) })
      }
      .distinctUntilChanged()
  }

  var downloadResult$: Observable<PhDownloadResult> { self.downloadResultS.asObservable() }

  func enqueue(_ request: PhDownloadRequest) -> Completable {
    Completable
      .deferred { [weak self] () -> Completable in
        guard let self = self else {
          return .error(PhDownloaderError.destroyed)
        }

        do {
          try self.dataSource.insertOrUpdate(
            identifier: request.identifier,
            url: request.url,
            fileName: request.fileName,
            savedDir: request.savedDir,
            state: .enqueued
          )
        } catch {
          return .error(error)
        }

        self.requestS.accept(request)
        return .empty()
      }
      .subscribeOn(concurrentMainScheduler)
  }

  func cancel(by identifier: String) -> Completable {
    fatalError()
  }

  func cancelAll() -> Completable {
    fatalError()
  }

}


